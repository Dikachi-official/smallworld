import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count
from django.utils import timezone

from rewards.models import Reward


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Find Reward rows claimed more than 7 days ago and report them. "
        "Pass --fix to mark them as 'expired'."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--fix",
            action="store_true",
            help="Apply changes: mark stale rewards as 'expired'. "
                 "Without this flag, nothing in the database is changed.",
        )

    def handle(self, *args, **options):
        fix = options["fix"]
        cutoff = timezone.now() - timedelta(days=7)

        # Query stale reward records
        stale_qs = Reward.objects.filter(
            status="claimed",
            claimed_at__lt=cutoff,
        )

        total = stale_qs.count()
        self.stdout.write(
            f"Found {total} stale reward(s) claimed before {cutoff.isoformat()}"
        )

        # Output breakdown grouped by reward_type
        breakdown = (
            stale_qs.values("reward_type")
            .annotate(count=Count("id"))
            .order_by("-count")
        )
        for row in breakdown:
            self.stdout.write(f"  {row['reward_type']}: {row['count']}")

        if total == 0:
            self.stdout.write(self.style.SUCCESS("Nothing to do."))
            return

        # Dry-run execution path
        if not fix:
            self.stdout.write(
                self.style.WARNING(
                    "Dry run only — no changes made. Re-run with --fix to update."
                )
            )
            return

        # Update execution path
        now = timezone.now()

        with transaction.atomic():
            # Lock target rows for safe update
            stale_rewards = list(stale_qs.select_for_update())
            
            for reward in stale_rewards:
                reward.status = "expired"
                reward.expires_at = now
                logger.info("Expired reward id=%s", reward.id)

            if stale_rewards:
                Reward.objects.bulk_update(stale_rewards, ["status", "expires_at"])

        self.stdout.write(
            self.style.SUCCESS(f"Updated {len(stale_rewards)} reward(s) to 'expired'.")
        )