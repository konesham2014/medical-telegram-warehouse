"""
Task 3: YOLO Object Detection for Data Enrichment

Uses YOLOv8 to analyze images from Telegram messages and classify them
into categories: promotional, product_display, lifestyle, or other.

Usage:
    python src/yolo_detect.py

Requirements:
    - ultralytics library installed
    - yolov8n.pt model (auto-downloaded on first run)
    - Images in data/raw/images/ directory
"""

import os
import csv
import logging
import glob
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple
from dotenv import load_dotenv
import psycopg2

# YOLO imports
from ultralytics import YOLO
import cv2

load_dotenv()

# Configuration
BASE_DIR = Path(__file__).parent.parent
IMAGE_DIR = BASE_DIR / 'data' / 'raw' / 'images'
OUTPUT_DIR = BASE_DIR / 'data' / 'processed'
LOG_DIR = BASE_DIR / 'logs'

for dir_path in [OUTPUT_DIR, LOG_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f"yolo_{datetime.now().strftime('%Y-%m-%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database config
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'medical_warehouse'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password')
}


class YOLODetector:
    """
    YOLOv8 object detection for medical product images.

    Classification scheme:
    - promotional: person + product (someone showing/holding item)
    - product_display: bottle/container, no person
    - lifestyle: person, no product
    - other: neither detected
    """

    # COCO classes relevant to our classification
    PERSON_CLASS = 0
    BOTTLE_CLASS = 39
    CUP_CLASS = 41
    # Additional relevant classes
    RELEVANT_CLASSES = {
        0: 'person',
        39: 'bottle', 
        41: 'cup',
        40: 'wine glass',
        74: 'clock',
        76: 'scissors',
        77: 'teddy bear',
        84: 'book'
    }

    def __init__(self, model_path: str = 'yolov8n.pt'):
        """
        Initialize YOLO detector.

        Args:
            model_path: Path to YOLOv8 model weights (auto-downloads if not found)
        """
        logger.info(f"Loading YOLO model: {model_path}")
        try:
            self.model = YOLO(model_path)
            logger.info("YOLO model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load YOLO model: {e}")
            raise

    def detect_image(self, image_path: str) -> Dict:
        """
        Run object detection on a single image.

        Args:
            image_path: Path to image file

        Returns:
            Dictionary with detection results
        """
        try:
            results = self.model(image_path, verbose=False)
            detections = []

            for result in results:
                boxes = result.boxes
                for box in boxes:
                    cls_id = int(box.cls[0])
                    confidence = float(box.conf[0])

                    if cls_id in self.RELEVANT_CLASSES:
                        detections.append({
                            'class_id': cls_id,
                            'class_name': self.RELEVANT_CLASSES[cls_id],
                            'confidence': round(confidence, 4)
                        })

            return {
                'image_path': image_path,
                'detections': detections,
                'has_person': any(d['class_id'] == self.PERSON_CLASS for d in detections),
                'has_product': any(d['class_id'] in [self.BOTTLE_CLASS, self.CUP_CLASS] for d in detections)
            }

        except Exception as e:
            logger.error(f"Detection failed for {image_path}: {e}")
            return {
                'image_path': image_path,
                'detections': [],
                'has_person': False,
                'has_product': False,
                'error': str(e)
            }

    def classify_image(self, detection_result: Dict) -> str:
        """
        Classify image based on detection results.

        Classification rules:
        - promotional: person + product detected
        - product_display: product detected, no person
        - lifestyle: person detected, no product
        - other: neither detected

        Args:
            detection_result: Result from detect_image()

        Returns:
            Classification category string
        """
        has_person = detection_result['has_person']
        has_product = detection_result['has_product']

        if has_person and has_product:
            return 'promotional'
        elif has_product and not has_person:
            return 'product_display'
        elif has_person and not has_product:
            return 'lifestyle'
        else:
            return 'other'

    def process_all_images(self) -> List[Dict]:
        """
        Process all images in the raw images directory.

        Returns:
            List of detection results with classifications
        """
        image_pattern = str(IMAGE_DIR / "**" / "*.jpg")
        image_files = glob.glob(image_pattern, recursive=True)

        logger.info(f"Found {len(image_files)} images to process")

        results = []
        for img_path in image_files:
            logger.info(f"Processing: {img_path}")

            detection = self.detect_image(img_path)
            category = self.classify_image(detection)

            # Extract message_id and channel from path
            path_parts = Path(img_path).parts
            channel_name = path_parts[-2] if len(path_parts) >= 2 else 'unknown'
            message_id = Path(img_path).stem

            result = {
                'message_id': message_id,
                'channel_name': channel_name,
                'image_path': img_path,
                'detected_class': detection['detections'][0]['class_name'] if detection['detections'] else 'none',
                'confidence_score': detection['detections'][0]['confidence'] if detection['detections'] else 0.0,
                'image_category': category,
                'has_person': detection['has_person'],
                'has_product': detection['has_product'],
                'total_detections': len(detection['detections'])
            }

            results.append(result)
            logger.info(f"Classified as: {category}")

        return results

    def save_to_csv(self, results: List[Dict], filename: str = 'detections.csv'):
        """
        Save detection results to CSV file.

        Args:
            results: List of detection result dictionaries
            filename: Output CSV filename
        """
        if not results:
            logger.warning("No results to save")
            return

        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)
            logger.info(f"Saved {len(results)} detections to {output_path}")
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")

    def load_to_postgres(self, results: List[Dict]):
        """
        Load detection results into PostgreSQL database.

        Args:
            results: List of detection result dictionaries
        """
        if not results:
            logger.warning("No results to load")
            return

        create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.yolo_detections (
            detection_id SERIAL PRIMARY KEY,
            message_id VARCHAR(50),
            channel_name VARCHAR(255),
            image_path TEXT,
            detected_class VARCHAR(50),
            confidence_score DECIMAL(5,4),
            image_category VARCHAR(50),
            has_person BOOLEAN,
            has_product BOOLEAN,
            total_detections INTEGER,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        insert_sql = """
        INSERT INTO staging.yolo_detections 
        (message_id, channel_name, image_path, detected_class, confidence_score, 
         image_category, has_person, has_product, total_detections)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            conn = psycopg2.connect(**DB_CONFIG)
            with conn.cursor() as cur:
                cur.execute(create_table_sql)

                for result in results:
                    cur.execute(insert_sql, (
                        result['message_id'],
                        result['channel_name'],
                        result['image_path'],
                        result['detected_class'],
                        result['confidence_score'],
                        result['image_category'],
                        result['has_person'],
                        result['has_product'],
                        result['total_detections']
                    ))

            conn.commit()
            logger.info(f"Loaded {len(results)} records into PostgreSQL")
            conn.close()

        except Exception as e:
            logger.error(f"Database error: {e}")


def main():
    """Main entry point for YOLO detection pipeline."""
    logger.info("Starting YOLO object detection pipeline")

    detector = YOLODetector()
    results = detector.process_all_images()

    if results:
        detector.save_to_csv(results)
        detector.load_to_postgres(results)

        # Print summary
        categories = {}
        for r in results:
            cat = r['image_category']
            categories[cat] = categories.get(cat, 0) + 1

        logger.info("Detection Summary:")
        for cat, count in categories.items():
            logger.info(f"  {cat}: {count}")
    else:
        logger.warning("No images found to process")

    logger.info("YOLO detection pipeline complete")


if __name__ == '__main__':
    main()
