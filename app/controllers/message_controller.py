"""
Message controller for handling message-related operations.
"""
from datetime import datetime
from typing import Optional, List
from fastapi import HTTPException, status

from app.models.cassandra_models import MessageModel
from app.schemas.message import (
    MessageCreate,
    MessageResponse,
    PaginatedMessageResponse
)

class MessageController:
    """
    Controller for handling message operations.
    """
    
    @staticmethod
    async def send_message(
        sender_id: int,
        receiver_id: int,
        content: str,
        conversation_id: Optional[int] = None
    ) -> MessageResponse:
        """
        Send a message to a user.
        
        Args:
            sender_id: ID of the sender
            receiver_id: ID of the receiver
            content: Message content
            conversation_id: Optional conversation ID
            
        Returns:
            Created message data
            
        Raises:
            HTTPException: If message creation fails
        """
        try:
            message = await MessageModel.create_message(
                sender_id=sender_id,
                receiver_id=receiver_id,
                content=content,
                conversation_id=conversation_id
            )
            return message
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to send message: {str(e)}"
            )
    
    @staticmethod
    async def get_conversation_messages(
        conversation_id: int,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages for a conversation with pagination.
        
        Args:
            conversation_id: ID of the conversation
            page: Page number (1-based)
            limit: Number of messages per page
            
        Returns:
            Paginated messages
            
        Raises:
            HTTPException: If message retrieval fails
        """
        try:
            messages = await MessageModel.get_conversation_messages(
                conversation_id=conversation_id,
                page=page,
                limit=limit
            )
            return messages
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get conversation messages: {str(e)}"
            )
    
    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: int,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages before a timestamp with pagination.
        
        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number (1-based)
            limit: Number of messages per page
            
        Returns:
            Paginated messages
            
        Raises:
            HTTPException: If message retrieval fails
        """
        try:
            messages = await MessageModel.get_messages_before_timestamp(
                conversation_id=conversation_id,
                before_timestamp=before_timestamp,
                page=page,
                limit=limit
            )
            return messages
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get messages before timestamp: {str(e)}"
            )