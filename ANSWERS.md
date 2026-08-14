### SmallWorld 

### Section 1: Debug This

### Question 1: Celery task silently fails on retry

**The Cause:**
1. When `run_ffmpeg()` fail permanently (e.g., corrupt video file), Celery retries up to `max_retries=3` and stops. However, `video.status` is only set to `'done'` upon success. After retries are exhausted, `video.status` remains in its initial pending/processing state indefinitely.
2. No error reaches sentry because standard Celery retry mechanism raises internal `Retry` exceptions rather than unhandled task exceptions. Once `max_retries` is exceeded without explicit error handling or logging, the exception isn't captured cleanly as an unhandled error state downstream.

**The Fix:**
import logging
from celery import shared_task

logger = logging.getLogger(__name__)

@shared_task(bind=True, max_retries=3)
def process_video(self, video_id):
    try:
        video = PostVideo.objects.get(id=video_id)
    except PostVideo.DoesNotExist:
        logger.warning("Video %s no longer exists", video_id)
        return

    try:
        run_ffmpeg(video.file_path)
        video.status = 'done'
        video.save(update_fields=['status'])
    except Exception as e:
        if self.request.retries >= self.max_retries:
            video.status = 'failed'
            video.save(update_fields=['status'])
            logger.error("Video %s failed after %d retries: %s", video_id, self.max_retries, e, exc_info=True)
            raise
        raise self.retry(exc=e, countdown=30)



### Question 2: Race condition in reward approval

**The Issue:**

Two concurrent requests both read the reward while status='claimed', both pass the check, and both call PaystackService.initiate_transfer() before either saves status='approved'. The database .get() uses a plain SELECT with no locking, so there's a race window between the read and write.

Result: Two transfers initiated for one reward.

**The Fix:**
Use database row locking (select_for_update()) and transition status to 'processing' before calling the external API to prevent duplicate payouts, while keeping external network calls outside DB transaction blocks.

from django.db import transaction
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def approve_reward(request, reward_id):
    # Lock row and transition status atomically
    with transaction.atomic():
        reward = Reward.objects.select_for_update().get(pk=reward_id)
        if reward.status != 'claimed':
            return Response({'error': 'Not claimable'}, status=400)
        
        reward.status = 'processing'
        reward.save(update_fields=['status'])

    # Call Paystack outsife the DB transaction lock
    try:
        result = PaystackService.initiate_transfer(
            amount=reward.amount,
            recipient=reward.paystack_recipient_code,
        )
        reward.status = 'approved'
        reward.transfer_code = result['transfer_code']
        reward.save(update_fields=['status', 'transfer_code'])
        return Response({'detail': 'Approved'})
    except Exception as e:
        # Revert status if transfer call fails
        reward.status = 'claimed'
        reward.save(update_fields=['status'])
        return Response({'error': f'Transfer failed: {str(e)}'}, status=500)



### Question 3: Migration will fail on a live table

**What Happens:**

Adding unique=True on a 500,000-row table without null=True or a default value causes PostgreSQL to fail immediately because existing rows have no value for the new column.

Even with a default value, applying a unique index blocks reads and writes on the target table while holding an exclusive lock, causing production downtime.

**Zero-Downtime Fix:**

Migration 1: Add field as nullable without constraints

class Migration(migrations.Migration):
    dependencies = [('post', '0059_previous')]
    
    operations = [
        migrations.AddField(
            model_name='post',
            name='content_hash',
            field=models.CharField(max_length=64, null=True, blank=True),
        )
    ]

Migration 2: Backfill existing data in batches

# Create a new empty migration, then add this function
def backfill_content_hash(apps, schema_editor):
    Post = apps.get_model('post', 'Post')
    # Batch by 1000 so we don't hold a long transaction
    for post in Post.objects.all().iterator(chunk_size=1000):
        post.content_hash = compute_hash(post.content)  # Your logic
        post.save(update_fields=['content_hash'])

class Migration(migrations.Migration):
    dependencies = [('post', '0060_add_content_hash')]
    
    operations = [
        migrations.RunPython(backfill_content_hash, migrations.RunPython.noop)
    ]

Migration 3: Build index concurrently and enforce uniqueness

class Migration(migrations.Migration):
    atomic = False  # Important: non-atomic for CONCURRENT index
    dependencies = [('post', '0061_backfill_content_hash')]
    
    operations = [
        # Build index without locking the table
        migrations.RunSQL(
            "CREATE UNIQUE INDEX CONCURRENTLY idx_post_content_hash ON post (content_hash);",
            reverse_sql="DROP INDEX IF EXISTS idx_post_content_hash;",
        ),
        # Then add the constraint using the pre-built index
        migrations.AddConstraint(
            model_name='post',
            constraint=models.UniqueConstraint(
                fields=['content_hash'],
                name='uq_post_content_hash'
            ),
        ),
    ]



## Section 2: Real Decisions

### Question 4: Celery task design (50,000 followers)

**Issues with the Naive Implementation:**

i. A single task iterating through 50,000 followers holds worker execution context for 10+ minutes.

ii.If an uncaught exception occurs midway, reexecuting the task sends duplicate push notifications to previously notified users.

iii. Exhausts push gateway (FCM/APNs) connection pools and rate limits.

Architecture: Fan Out Pattern
[Post Published Event] 
        │
[Dispatcher Task] ──(Chunks follower IDs into batches of 500)
        │
        ├──> [Batch Notification Task 1 (500 users)]
        ├──> [Batch Notification Task 2 (500 users)]
        └──> [Batch Notification Task N (500 users)]

**Code Design:**

@shared_task
def notify_post_published(post_id):
    """Dispatcher task: retrieves follower IDs and dispatches chunked batch tasks."""
    post = Post.objects.get(id=post_id)
    follower_ids = (
        Follower.objects.filter(creator_id=post.author_id)
        .values_list('follower_id', flat=True)
        .iterator(chunk_size=1000)
    )
    
    batch = []
    batch_size = 500
    for follower_id in follower_ids:
        batch.append(follower_id)
        if len(batch) >= batch_size:
            send_batch_notifications.delay(post_id, batch)
            batch = []
            
    if batch:
        send_batch_notifications.delay(post_id, batch)

@shared_task(rate_limit='100/m', queue='notifications')
def send_batch_notifications(post_id, follower_ids):
    """Worker task: executes notification sends with idempotency checks."""
    post = Post.objects.get(id=post_id)
    
    for follower_id in follower_ids:
        _, created = NotificationSent.objects.get_or_create(
            post_id=post_id,
            user_id=follower_id,
        )
        if not created:
            continue  # Prevents duplicate delivery on retries
            
        try:
            push_provider.send(
                user_id=follower_id,
                title=post.title,
                message=post.summary,
            )
        except Exception as e:
            logger.error("Failed sending notification to user %s: %s", follower_id, e)



### Question 5: Database index decision
i. The Query
SupportTicket.objects.filter(
    status='open',
    assigned_operator=request.user
).order_by('-created_at')[:20]

ii. Index:
A composite index covering (assigned_operator_id, status, created_at DESC).
class SupportTicket(models.Model):
    status = models.CharField(max_length=20)
    assigned_operator = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        indexes = [
            models.Index(
                fields=['assigned_operator', 'status', '-created_at'],
                name='idx_ticket_operator_status_created',
            ),
        ]
**Why This Index:**
i. assigned_operator_id and status satisfy exact equality filtering (WHERE assigned_operator_id = X AND status = 'open').
ii. Including -created_at in the composite index allows PostgreSQL to satisfy ORDER BY created_at DESC directly from the index order without executing an expensive in-memory sort step (Sort).



### Question 6: Debugging a production spike
A SIGKILL (Signal 9) accompanied by a CPU spike is a signature of the Linux Out-Of-Memory (OOM) Killer terminating worker processes consuming excessive memory.

**First Three Diagnostic Checks:**

1. Kernel OOM Logs (/var/log/dmesg or journalctl -k):
    Why: Confirms whether the kernel invoked the OOM killer due to memory exhaustion vs an external process signal. Look for entries containing Out of memory:
    Kill process <pid> (celery).

2. Celery Task Logs Around 04:00:
    Why: Identifies exact tasks initiated at or immediately prior to 4:00 AM (e.g., scheduled Celery Beat cron jobs, unbounded query evaluations, or large file 
    processing tasks loading entire datasets into memory).

3. CloudWatch CPU/RAM Metrics & Database Query Metrics:
    Why: Correlates the 4 AM CPU spike with database connection spikes, long-running queries, or unpaginated query evaluations occurring concurrently.



### Question 7: Security review (password reset endpoint)
Issues:
i. User Enumeration
    Risk:
    404 Not Found vs 200 OK reveals valid registered user emails to attackers.
    Security Fix:
    Return uniform generic message regardless of email existence.

ii. Weak Token Space
    Risk:
    4-digit numbers (1000–9999) can be brute-forced within seconds.
    Security Fix:
    Generate high-entropy tokens using secrets.token_urlsafe(32).

iii. Insecure PRNG
    Risk:
    "Python's standard random module uses Mersenne Twister, which is deterministic."
    Security Fix:
    Use cryptographically secure module secrets.

iv. Missing Expiration
    Risk:
    Unused reset tokens remain valid indefinitely.
    Security Fix:
    "Save reset_token_expires_at timestamp (e.g., 15 minutes validity)."

v. Plaintext Storage
    Risk:
    Compromised database leaks valid active password reset tokens.
    Security Fix:
    Store token as salted hash using make_password().

vi Missing Rate Limits
    Risk:
    Allows automated brute-force attempts and spam resource exhaustion.
    Security Fix:
    Apply IP and account throttle decorators (@ratelimit).

vii. Missing Invalidation
    Risk:
    Reset tokens can be reused multiple times.
    Security Fix:
    Clear reset_token field immediately upon password change completion.



## Section 3: Write It

See `section_3_management_command/audit_stale_rewards.py`.
