"""
Conversation controller for handling conversation-related operations.
"""
from typing import Optional
from fastapi import HTTPException, status

from app.models.cassandra_models import ConversationModel
from app.schemas.conversation import (
    ConversationResponse,
    PaginatedConversationResponse
)

class ConversationController:
    """
    Controller for handling conversation operations.
    """
    
    @staticmethod
    async def get_user_conversations(
        user_id: int,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get conversations for a user with pagination.
        
        Args:
            user_id: ID of the user
            page: Page number (1-based)
            limit: Number of conversations per page
            
        Returns:
            Paginated conversations
            
        Raises:
            HTTPException: If conversation retrieval fails
        """
        try:
            conversations = await ConversationModel.get_user_conversations(
                user_id=user_id,
                page=page,
                limit=limit
            )
            return conversations
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get user conversations: {str(e)}"
            )
    
    @staticmethod
    async def get_conversation(conversation_id: int) -> ConversationResponse:
        """
        Get a conversation by ID.
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Conversation data
            
        Raises:
            HTTPException: If conversation not found or retrieval fails
        """
        try:
            conversation = await ConversationModel.get_conversation(
                conversation_id=conversation_id
            )
            return conversation
        except Exception as e:
            if "not found" in str(e).lower():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=str(e)
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get conversation: {str(e)}"
            )
    
    @staticmethod
    async def create_or_get_conversation(
        sender_id: int,
        receiver_id: int
    ) -> int:
        """
        Get an existing conversation between two users or create a new one.
        
        Args:
            sender_id: ID of the sender
            receiver_id: ID of the receiver
            
        Returns:
            Conversation ID
            
        Raises:
            HTTPException: If conversation creation/retrieval fails
        """
        try:
            conversation_id = await ConversationModel.create_or_get_conversation(
                sender_id=sender_id,
                receiver_id=receiver_id
            )
            return conversation_id
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create or get conversation: {str(e)}"
            )