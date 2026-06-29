
"""
Task 4: Pydantic Schemas for API Request/Response Validation
"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, date


class TopProductsRequest(BaseModel):
    """Request parameters for top products endpoint."""
    limit: int = Field(default=10, ge=1, le=100, description="Number of top products to return")
    channel_name: Optional[str] = Field(default=None, description="Filter by specific channel")


class ChannelActivityRequest(BaseModel):
    """Request parameters for channel activity endpoint."""
    channel_name: str = Field(..., description="Name of the channel to analyze")
    start_date: Optional[date] = Field(default=None, description="Start date for analysis period")
    end_date: Optional[date] = Field(default=None, description="End date for analysis period")


class MessageSearchRequest(BaseModel):
    """Request parameters for message search endpoint."""
    query: str = Field(..., min_length=1, max_length=200, description="Search keyword")
    limit: int = Field(default=20, ge=1, le=100, description="Maximum results to return")
    channel_name: Optional[str] = Field(default=None, description="Filter by channel")


class TopProductItem(BaseModel):
    """Single top product item."""
    product_name: str = Field(..., description="Name of the product/term")
    mention_count: int = Field(..., description="Number of times mentioned")
    avg_views: float = Field(..., description="Average views for messages mentioning this product")
    channels: List[str] = Field(..., description="Channels where this product was mentioned")


class TopProductsResponse(BaseModel):
    """Response for top products endpoint."""
    products: List[TopProductItem]
    total_analyzed: int = Field(..., description="Total messages analyzed")
    period: str = Field(..., description="Analysis period description")


class DailyActivity(BaseModel):
    """Daily posting activity metrics."""
    date: date
    message_count: int
    total_views: int
    avg_views: float
    top_product: Optional[str]


class ChannelActivityResponse(BaseModel):
    """Response for channel activity endpoint."""
    channel_name: str
    total_posts: int
    avg_views: float
    total_images: int
    daily_activity: List[DailyActivity]
    posting_trend: str = Field(..., description="Trend description: increasing, decreasing, stable")


class MessageItem(BaseModel):
    """Single message in search results."""
    message_id: int
    channel_name: str
    message_date: datetime
    message_text: str
    view_count: int
    forward_count: int
    has_image: bool


class MessageSearchResponse(BaseModel):
    """Response for message search endpoint."""
    query: str
    results: List[MessageItem]
    total_found: int
    limit: int


class VisualContentStats(BaseModel):
    """Visual content statistics for a channel."""
    channel_name: str
    total_messages: int
    messages_with_images: int
    image_percentage: float
    promotional_count: int
    product_display_count: int
    lifestyle_count: int
    other_count: int


class VisualContentResponse(BaseModel):
    """Response for visual content statistics endpoint."""
    overall_stats: dict
    channel_breakdown: List[VisualContentStats]
    image_category_distribution: dict


class HealthCheck(BaseModel):
    """API health check response."""
    status: str
    version: str
    database_connected: bool
    timestamp: datetime
