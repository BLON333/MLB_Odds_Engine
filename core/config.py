"""Global configuration flags controlling console output verbosity."""

DEBUG_MODE = False
VERBOSE_MODE = False

# Discord webhook for snapshot dispatching
import os

DISCORD_SNAPSHOT_WEBHOOK = os.getenv(
    "DISCORD_SNAPSHOT_WEBHOOK",
    "https://discord.com/api/webhooks/your-webhook-id",
)
