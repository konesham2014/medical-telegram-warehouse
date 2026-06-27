"""
Task 1: Telegram Data Scraper
Extracts messages and images from Telegram channels and stores them in a raw data lake.

Usage:
    python src/scraper.py

Requirements:
    - Telegram API credentials (API_ID, API_HASH) from my.telegram.org
    - Telethon library installed
"""

import os
import json
import logging
import asyncio
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto

# Load environment variables
load_dotenv()

# Configuration
API_ID = int(os.getenv('TELEGRAM_API_ID', '0'))
API_HASH = os.getenv('TELEGRAM_API_HASH', '')
PHONE = os.getenv('TELEGRAM_PHONE', '')

# Channels to scrape (Ethiopian medical/pharmaceutical channels)
CHANNELS = [
    'CheMed123',           # CheMed - Medical products
    'lobelia_cosmetics',   # Lobelia Cosmetics
    'tikvahpharma',        # Tikvah Pharma
    # Additional channels can be added from et.tgstat.com/medicine
]

# Directory structure
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data' / 'raw'
IMAGE_DIR = DATA_DIR / 'images'
MESSAGE_DIR = DATA_DIR / 'telegram_messages'
LOG_DIR = BASE_DIR / 'logs'

# Ensure directories exist
for dir_path in [IMAGE_DIR, MESSAGE_DIR, LOG_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Logging setup
log_file = LOG_DIR / f"scraper_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TelegramScraper:
    """
    Scrapes messages and media from public Telegram channels.

    Attributes:
        client: Telethon TelegramClient instance
        channels: List of channel names/usernames to scrape
    """

    def __init__(self, api_id: int, api_hash: str, phone: str, channels: list):
        self.client = TelegramClient('session_name', api_id, api_hash)
        self.phone = phone
        self.channels = channels
        self.scraped_data = []

    async def connect(self):
        """Connect to Telegram API and authenticate."""
        await self.client.start(phone=self.phone)
        logger.info("Connected to Telegram API successfully")

    async def scrape_channel(self, channel_name: str, limit: int = 100):
        """
        Scrape messages from a specific channel.

        Args:
            channel_name: Telegram channel username or ID
            limit: Maximum number of messages to scrape

        Returns:
            List of message dictionaries
        """
        messages = []
        channel_name_clean = channel_name.replace('@', '').replace('https://t.me/', '')

        try:
            logger.info(f"Starting scrape for channel: {channel_name}")
            entity = await self.client.get_entity(channel_name)

            async for message in self.client.iter_messages(entity, limit=limit):
                if not message.text and not message.media:
                    continue

                msg_data = {
                    'message_id': message.id,
                    'channel_name': channel_name_clean,
                    'message_date': message.date.isoformat() if message.date else None,
                    'message_text': message.text or '',
                    'views': message.views or 0,
                    'forwards': message.forwards or 0,
                    'has_media': message.media is not None,
                    'image_path': None
                }

                # Download images if present
                if message.media and isinstance(message.media, MessageMediaPhoto):
                    image_path = await self._download_image(message, channel_name_clean)
                    msg_data['image_path'] = image_path

                messages.append(msg_data)

            logger.info(f"Scraped {len(messages)} messages from {channel_name}")

        except Exception as e:
            logger.error(f"Error scraping {channel_name}: {str(e)}")

        return messages

    async def _download_image(self, message, channel_name: str) -> str:
        """
        Download image from message and save to organized directory.

        Args:
            message: Telethon message object
            channel_name: Name of the channel

        Returns:
            Relative path to saved image
        """
        channel_dir = IMAGE_DIR / channel_name
        channel_dir.mkdir(parents=True, exist_ok=True)

        image_filename = f"{message.id}.jpg"
        image_path = channel_dir / image_filename

        try:
            await message.download_media(file=str(image_path))
            logger.info(f"Downloaded image: {image_path}")
            return f"data/raw/images/{channel_name}/{image_filename}"
        except Exception as e:
            logger.error(f"Failed to download image for message {message.id}: {str(e)}")
            return None

    def save_to_json(self, messages: list, channel_name: str):
        """
        Save scraped messages to JSON files in partitioned directory structure.

        Structure: data/raw/telegram_messages/YYYY-MM-DD/channel_name.json

        Args:
            messages: List of message dictionaries
            channel_name: Name of the channel
        """
        if not messages:
            logger.warning(f"No messages to save for {channel_name}")
            return

        today = datetime.now().strftime('%Y-%m-%d')
        date_dir = MESSAGE_DIR / today
        date_dir.mkdir(parents=True, exist_ok=True)

        filename = date_dir / f"{channel_name}.json"

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(messages)} messages to {filename}")
        except Exception as e:
            logger.error(f"Error saving to JSON: {str(e)}")

    async def run(self, limit: int = 100):
        """
        Run the scraper for all configured channels.

        Args:
            limit: Maximum messages per channel
        """
        await self.connect()

        for channel in self.channels:
            messages = await self.scrape_channel(channel, limit=limit)
            self.scraped_data.extend(messages)
            self.save_to_json(messages, channel.replace('@', ''))

            # Rate limiting - sleep between channels
            await asyncio.sleep(2)

        logger.info(f"Scraping complete. Total messages: {len(self.scraped_data)}")
        await self.client.disconnect()


def main():
    """Main entry point for the scraper."""
    if API_ID == 0 or not API_HASH:
        logger.error("Telegram API credentials not configured. Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env file")
        return

    scraper = TelegramScraper(API_ID, API_HASH, PHONE, CHANNELS)
    asyncio.run(scraper.run(limit=100))


if __name__ == '__main__':
    main()
