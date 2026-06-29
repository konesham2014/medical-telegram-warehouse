
"""
Task 4: FastAPI Analytical API

Exposes the data warehouse through REST API endpoints that answer
key business questions about Ethiopian medical Telegram channels.

Run with: uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
API Docs: http://localhost:8000/docs
"""

import os
import re
from collections import Counter
from datetime import datetime, date, timedelta
from typing import Optional, List
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import Session

from api.database import get_db
from api.schemas import (
    TopProductsResponse, TopProductItem,
    ChannelActivityResponse, DailyActivity,
    MessageSearchResponse, MessageItem,
    VisualContentResponse, VisualContentStats,
    HealthCheck
)

load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Medical Telegram Analytics API",
    description="""
    Analytical API for Ethiopian Medical Telegram Channels Data Warehouse.

    Provides insights into:
    - Top mentioned medical products
    - Channel posting activity and trends
    - Message search capabilities
    - Visual content statistics
    """,
    version="1.0.0",
    contact={
        "name": "Kara Solutions Data Engineering Team",
        "email": "data@karasolutions.et"
    }
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== HEALTH CHECK ==============

@app.get("/", response_model=HealthCheck, tags=["Health"])
async def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint to verify API and database connectivity.

    Returns:
        HealthCheck: Status information about the API
    """
    try:
        db.execute(text("SELECT 1"))
        db_connected = True
    except Exception:
        db_connected = False

    return HealthCheck(
        status="healthy",
        version="1.0.0",
        database_connected=db_connected,
        timestamp=datetime.utcnow()
    )


# ============== ENDPOINT 1: TOP PRODUCTS ==============

@app.get("/api/reports/top-products", response_model=TopProductsResponse, tags=["Reports"])
async def get_top_products(
    limit: int = Query(default=10, ge=1, le=100, description="Number of top products to return"),
    channel_name: Optional[str] = Query(default=None, description="Filter by specific channel"),
    db: Session = Depends(get_db)
):
    """
    Returns the most frequently mentioned medical products/terms across all channels.

    Uses regex pattern matching to extract product names from message text.
    Filters out common stop words and focuses on medical/pharmaceutical terms.

    Args:
        limit: Number of top products to return (1-100)
        channel_name: Optional filter for specific channel

    Returns:
        TopProductsResponse: List of top products with mention counts and avg views
    """
    try:
        # Query to get all messages with text
        query = """
        SELECT message_text, view_count, channel_name
        FROM marts.fct_messages fm
        JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE message_text IS NOT NULL AND LENGTH(message_text) > 0
        """

        params = {}
        if channel_name:
            query += " AND dc.channel_name = :channel_name"
            params['channel_name'] = channel_name.lower()

        result = db.execute(text(query), params)
        messages = result.fetchall()

        if not messages:
            return TopProductsResponse(
                products=[],
                total_analyzed=0,
                period="all time"
            )

        # Medical product keywords to look for
        medical_keywords = [
            'paracetamol', 'ibuprofen', 'amoxicillin', 'metformin', 'aspirin',
            'vitamin', 'supplement', 'antibiotic', 'cream', 'ointment', 'syrup',
            'tablet', 'capsule', 'injection', 'vaccine', 'insulin', 'antimalarial',
            'cough', 'fever', 'pain', 'allergy', 'diabetes', 'hypertension',
            'malaria', 'covid', 'mask', 'sanitizer', 'disinfectant',
            'cosmetic', 'lotion', 'shampoo', 'soap', 'perfume',
            'face cream', 'body lotion', 'hair oil', 'skin care'
        ]

        # Extract mentions
        product_mentions = Counter()
        product_views = {}
        product_channels = {}

        for msg in messages:
            text_content = msg[0].lower()
            views = msg[1] or 0
            ch_name = msg[2]

            for keyword in medical_keywords:
                if keyword in text_content:
                    product_mentions[keyword] += 1
                    if keyword not in product_views:
                        product_views[keyword] = []
                        product_channels[keyword] = set()
                    product_views[keyword].append(views)
                    product_channels[keyword].add(ch_name)

        # Build response
        top_products = []
        for product, count in product_mentions.most_common(limit):
            avg_views = sum(product_views[product]) / len(product_views[product]) if product_views[product] else 0
            top_products.append(TopProductItem(
                product_name=product.title(),
                mention_count=count,
                avg_views=round(avg_views, 2),
                channels=list(product_channels[product])
            ))

        return TopProductsResponse(
            products=top_products,
            total_analyzed=len(messages),
            period="all time"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing products: {str(e)}")


# ============== ENDPOINT 2: CHANNEL ACTIVITY ==============

@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivityResponse, tags=["Channels"])
async def get_channel_activity(
    channel_name: str,
    start_date: Optional[date] = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(default=None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Returns posting activity and trends for a specific channel.

    Provides daily breakdown of message counts, views, and identifies
    posting trends (increasing, decreasing, or stable).

    Args:
        channel_name: Name of the Telegram channel
        start_date: Optional start date filter
        end_date: Optional end date filter

    Returns:
        ChannelActivityResponse: Detailed activity metrics and trends
    """
    try:
        # Get channel info
        channel_query = """
        SELECT channel_key, total_posts, avg_views, total_images
        FROM marts.dim_channels
        WHERE channel_name = :channel_name
        """
        channel_result = db.execute(text(channel_query), {'channel_name': channel_name.lower()})
        channel_info = channel_result.fetchone()

        if not channel_info:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found")

        channel_key = channel_info[0]

        # Build date filter
        date_filter = ""
        params = {'channel_key': channel_key}

        if start_date:
            date_filter += " AND DATE(original_timestamp) >= :start_date"
            params['start_date'] = start_date
        if end_date:
            date_filter += " AND DATE(original_timestamp) <= :end_date"
            params['end_date'] = end_date

        # Daily activity query
        daily_query = f"""
        SELECT 
            DATE(original_timestamp) as msg_date,
            COUNT(*) as msg_count,
            COALESCE(SUM(view_count), 0) as total_views,
            COALESCE(AVG(view_count), 0) as avg_views
        FROM marts.fct_messages
        WHERE channel_key = :channel_key {date_filter}
        GROUP BY DATE(original_timestamp)
        ORDER BY msg_date
        """

        daily_result = db.execute(text(daily_query), params)
        daily_rows = daily_result.fetchall()

        if not daily_rows:
            raise HTTPException(status_code=404, detail="No activity found for the specified period")

        # Calculate trend
        if len(daily_rows) >= 2:
            first_half = sum(row[1] for row in daily_rows[:len(daily_rows)//2])
            second_half = sum(row[1] for row in daily_rows[len(daily_rows)//2:])

            if second_half > first_half * 1.1:
                trend = "increasing"
            elif second_half < first_half * 0.9:
                trend = "decreasing"
            else:
                trend = "stable"
        else:
            trend = "stable"

        daily_activity = [
            DailyActivity(
                date=row[0],
                message_count=row[1],
                total_views=row[2],
                avg_views=round(row[3], 2),
                top_product=None
            )
            for row in daily_rows
        ]

        return ChannelActivityResponse(
            channel_name=channel_name,
            total_posts=channel_info[1],
            avg_views=round(channel_info[2], 2),
            total_images=channel_info[3],
            daily_activity=daily_activity,
            posting_trend=trend
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching channel activity: {str(e)}")


# ============== ENDPOINT 3: MESSAGE SEARCH ==============

@app.get("/api/search/messages", response_model=MessageSearchResponse, tags=["Search"])
async def search_messages(
    query: str = Query(..., min_length=1, max_length=200, description="Search keyword"),
    limit: int = Query(default=20, ge=1, le=100, description="Maximum results"),
    channel_name: Optional[str] = Query(default=None, description="Filter by channel"),
    db: Session = Depends(get_db)
):
    """
    Searches for messages containing a specific keyword.

    Performs case-insensitive search on message text content.
    Supports filtering by channel and pagination via limit.

    Args:
        query: Search keyword (required)
        limit: Maximum number of results (1-100)
        channel_name: Optional channel filter

    Returns:
        MessageSearchResponse: Matching messages with metadata
    """
    try:
        search_query = """
        SELECT 
            fm.message_id,
            dc.channel_name,
            fm.original_timestamp as message_date,
            fm.message_text,
            fm.view_count,
            fm.forward_count,
            fm.has_image_flag
        FROM marts.fct_messages fm
        JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE LOWER(fm.message_text) LIKE LOWER(:search_pattern)
        """

        params = {'search_pattern': f'%{query}%', 'limit': limit}

        if channel_name:
            search_query += " AND dc.channel_name = :channel_name"
            params['channel_name'] = channel_name.lower()

        search_query += " ORDER BY fm.original_timestamp DESC LIMIT :limit"

        result = db.execute(text(search_query), params)
        rows = result.fetchall()

        messages = [
            MessageItem(
                message_id=row[0],
                channel_name=row[1],
                message_date=row[2],
                message_text=row[3][:500] + "..." if len(row[3]) > 500 else row[3],
                view_count=row[4] or 0,
                forward_count=row[5] or 0,
                has_image=row[6] or False
            )
            for row in rows
        ]

        # Get total count
        count_query = """
        SELECT COUNT(*) 
        FROM marts.fct_messages fm
        JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE LOWER(fm.message_text) LIKE LOWER(:search_pattern)
        """
        count_params = {'search_pattern': f'%{query}%'}

        if channel_name:
            count_query += " AND dc.channel_name = :channel_name"
            count_params['channel_name'] = channel_name.lower()

        count_result = db.execute(text(count_query), count_params)
        total_found = count_result.scalar()

        return MessageSearchResponse(
            query=query,
            results=messages,
            total_found=total_found,
            limit=limit
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching messages: {str(e)}")


# ============== ENDPOINT 4: VISUAL CONTENT STATS ==============

@app.get("/api/reports/visual-content", response_model=VisualContentResponse, tags=["Reports"])
async def get_visual_content_stats(db: Session = Depends(get_db)):
    """
    Returns statistics about image usage across all channels.

    Analyzes YOLO detection results to provide insights into:
    - Image usage percentage per channel
    - Category distribution (promotional, product_display, lifestyle, other)
    - Overall visual content patterns

    Returns:
        VisualContentResponse: Comprehensive visual content statistics
    """
    try:
        # Overall stats
        overall_query = """
        SELECT 
            COUNT(*) as total_messages,
            SUM(CASE WHEN has_image_flag THEN 1 ELSE 0 END) as messages_with_images
        FROM marts.fct_messages
        """
        overall_result = db.execute(text(overall_query))
        overall = overall_result.fetchone()

        total_messages = overall[0] or 0
        messages_with_images = overall[1] or 0

        # Channel breakdown
        channel_query = """
        SELECT 
            dc.channel_name,
            COUNT(*) as total_messages,
            SUM(CASE WHEN fm.has_image_flag THEN 1 ELSE 0 END) as messages_with_images,
            COALESCE(SUM(CASE WHEN fid.image_category = 'promotional' THEN 1 ELSE 0 END), 0) as promotional,
            COALESCE(SUM(CASE WHEN fid.image_category = 'product_display' THEN 1 ELSE 0 END), 0) as product_display,
            COALESCE(SUM(CASE WHEN fid.image_category = 'lifestyle' THEN 1 ELSE 0 END), 0) as lifestyle,
            COALESCE(SUM(CASE WHEN fid.image_category = 'other' THEN 1 ELSE 0 END), 0) as other
        FROM marts.dim_channels dc
        LEFT JOIN marts.fct_messages fm ON dc.channel_key = fm.channel_key
        LEFT JOIN marts.fct_image_detections fid ON fm.message_id = fid.message_id
        GROUP BY dc.channel_name
        ORDER BY total_messages DESC
        """

        channel_result = db.execute(text(channel_query))
        channel_rows = channel_result.fetchall()

        channel_breakdown = []
        for row in channel_rows:
            total = row[1] or 1  # Avoid division by zero
            channel_breakdown.append(VisualContentStats(
                channel_name=row[0],
                total_messages=row[1] or 0,
                messages_with_images=row[2] or 0,
                image_percentage=round((row[2] or 0) / total * 100, 2),
                promotional_count=row[3],
                product_display_count=row[4],
                lifestyle_count=row[5],
                other_count=row[6]
            ))

        # Category distribution
        category_query = """
        SELECT 
            image_category,
            COUNT(*) as count
        FROM marts.fct_image_detections
        WHERE image_category IS NOT NULL
        GROUP BY image_category
        """
        category_result = db.execute(text(category_query))
        category_distribution = {row[0]: row[1] for row in category_result.fetchall()}

        return VisualContentResponse(
            overall_stats={
                "total_messages": total_messages,
                "messages_with_images": messages_with_images,
                "overall_image_percentage": round(messages_with_images / total_messages * 100, 2) if total_messages > 0 else 0
            },
            channel_breakdown=channel_breakdown,
            image_category_distribution=category_distribution
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching visual content stats: {str(e)}")


# ============== ADDITIONAL ENDPOINTS ==============

@app.get("/api/channels", tags=["Channels"])
async def list_channels(db: Session = Depends(get_db)):
    """List all available channels in the data warehouse."""
    try:
        query = "SELECT channel_name, channel_type, total_posts, avg_views FROM marts.dim_channels ORDER BY total_posts DESC"
        result = db.execute(text(query))
        channels = [
            {
                "channel_name": row[0],
                "channel_type": row[1],
                "total_posts": row[2],
                "avg_views": round(row[3], 2)
            }
            for row in result.fetchall()
        ]
        return {"channels": channels}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports/daily-trends", tags=["Reports"])
async def get_daily_trends(
    days: int = Query(default=30, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Returns daily posting volume trends for health-related topics.

    Args:
        days: Number of recent days to analyze (1-365)

    Returns:
        Daily message counts and view trends
    """
    try:
        query = """
        SELECT 
            dd.full_date,
            COUNT(fm.message_id) as message_count,
            COALESCE(SUM(fm.view_count), 0) as total_views,
            COALESCE(AVG(fm.view_count), 0) as avg_views
        FROM marts.dim_dates dd
        LEFT JOIN marts.fct_messages fm ON dd.date_key = fm.date_key
        WHERE dd.full_date >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY dd.full_date
        ORDER BY dd.full_date
        """ % days

        result = db.execute(text(query))
        trends = [
            {
                "date": row[0].isoformat() if row[0] else None,
                "message_count": row[1],
                "total_views": row[2],
                "avg_views": round(row[3], 2)
            }
            for row in result.fetchall()
        ]

        return {
            "period_days": days,
            "trends": trends
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
