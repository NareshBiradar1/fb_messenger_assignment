"""
Models for interacting with Cassandra tables.
"""
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client
from app.schemas.message import MessageResponse, PaginatedMessageResponse
from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse

class MessageModel:
    """
    Message model for interacting with the messages table.
    """
    
    @staticmethod
    async def create_message(
        sender_id: int,
        receiver_id: int,
        content: str,
        conversation_id: Optional[int] = None
    ) -> MessageResponse:
        """
        Create a new message.
        
        Args:
            sender_id: ID of the sender
            receiver_id: ID of the receiver
            content: Message content
            conversation_id: Optional conversation ID (if not provided, will be generated)
            
        Returns:
            Created message data
        """
        try:
            # Get or create conversation_id
            if not conversation_id:
                # Get sender's conversations
                sender_data = cassandra_client.execute("""
                    SELECT conversations FROM users
                    WHERE user_id = %(user_id)s
                """, {'user_id': sender_id})
                
                if sender_data and sender_data[0]['conversations']:
                    # Check if conversation exists with receiver
                    conversation_id = sender_data[0]['conversations'].get(receiver_id)
                
                if not conversation_id:
                    # Generate new conversation_id
                    conversation_id = int(uuid.uuid4().int & ((1 << 31) - 1))
                    
                    # Update sender's conversations
                    cassandra_client.execute("""
                        UPDATE users
                        SET conversations[%(receiver_id)s] = %(conversation_id)s
                        WHERE user_id = %(sender_id)s
                    """, {
                        'sender_id': sender_id,
                        'receiver_id': receiver_id,
                        'conversation_id': conversation_id
                    })
                    
                    # Update receiver's conversations
                    cassandra_client.execute("""
                        UPDATE users
                        SET conversations[%(sender_id)s] = %(conversation_id)s
                        WHERE user_id = %(receiver_id)s
                    """, {
                        'receiver_id': receiver_id,
                        'sender_id': sender_id,
                        'conversation_id': conversation_id
                    })
            
            # Insert message
            message_id = uuid.uuid4()
            created_at = datetime.utcnow()
            
            cassandra_client.execute("""
                INSERT INTO conversations (
                    conversation_id, created_at, message_id,
                    sender_id, receiver_id, content
                ) VALUES (
                    %(conversation_id)s, %(created_at)s, %(message_id)s,
                    %(sender_id)s, %(receiver_id)s, %(content)s
                )
            """, {
                'conversation_id': conversation_id,
                'created_at': created_at,
                'message_id': message_id,
                'sender_id': sender_id,
                'receiver_id': receiver_id,
                'content': content
            })
            
            return MessageResponse(
                id=message_id,
                conversation_id=conversation_id,
                sender_id=sender_id,
                receiver_id=receiver_id,
                content=content,
                created_at=created_at
            )
            
        except Exception as e:
            raise Exception(f"Failed to create message: {str(e)}")
    
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
            Dictionary containing messages and pagination info
        """
        try:
            offset = (page - 1) * limit
            
            # Get total count
            count_result = cassandra_client.execute("""
                SELECT COUNT(*) as count FROM conversations
                WHERE conversation_id = %(conversation_id)s
            """, {'conversation_id': conversation_id})
            
            total_count = count_result[0]['count'] if count_result else 0
            
            # Get messages
            messages = cassandra_client.execute("""
                SELECT * FROM conversations
                WHERE conversation_id = %(conversation_id)s
                ORDER BY created_at DESC, message_id DESC
                LIMIT %(limit)s
                OFFSET %(offset)s
            """, {
                'conversation_id': conversation_id,
                'limit': limit,
                'offset': offset
            })
            
            return PaginatedMessageResponse(
                total=total_count,
                page=page,
                limit=limit,
                data=[
                    MessageResponse(
                        id=msg['message_id'],
                        conversation_id=msg['conversation_id'],
                        sender_id=msg['sender_id'],
                        receiver_id=msg['receiver_id'],
                        content=msg['content'],
                        created_at=msg['created_at']
                    ) for msg in messages
                ]
            )
            
        except Exception as e:
            raise Exception(f"Failed to get conversation messages: {str(e)}")
    
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
            Dictionary containing messages and pagination info
        """
        try:
            offset = (page - 1) * limit
            
            # Get total count
            count_result = cassandra_client.execute("""
                SELECT COUNT(*) as count FROM conversations
                WHERE conversation_id = %(conversation_id)s
                AND created_at < %(before_timestamp)s
            """, {
                'conversation_id': conversation_id,
                'before_timestamp': before_timestamp
            })
            
            total_count = count_result[0]['count'] if count_result else 0
            
            # Get messages
            messages = cassandra_client.execute("""
                SELECT * FROM conversations
                WHERE conversation_id = %(conversation_id)s
                AND created_at < %(before_timestamp)s
                ORDER BY created_at DESC, message_id DESC
                LIMIT %(limit)s
                OFFSET %(offset)s
            """, {
                'conversation_id': conversation_id,
                'before_timestamp': before_timestamp,
                'limit': limit,
                'offset': offset
            })
            
            return PaginatedMessageResponse(
                total=total_count,
                page=page,
                limit=limit,
                data=[
                    MessageResponse(
                        id=msg['message_id'],
                        conversation_id=msg['conversation_id'],
                        sender_id=msg['sender_id'],
                        receiver_id=msg['receiver_id'],
                        content=msg['content'],
                        created_at=msg['created_at']
                    ) for msg in messages
                ]
            )
            
        except Exception as e:
            raise Exception(f"Failed to get messages before timestamp: {str(e)}")


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
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
            Dictionary containing conversations and pagination info
        """
        try:
            # Get user's conversations map
            user_data = cassandra_client.execute("""
                SELECT conversations FROM users
                WHERE user_id = %(user_id)s
            """, {'user_id': user_id})
            
            if not user_data or not user_data[0]['conversations']:
                return PaginatedConversationResponse(
                    total=0,
                    page=page,
                    limit=limit,
                    data=[]
                )
            
            conversations_map = user_data[0]['conversations']
            total_count = len(conversations_map)
            
            # Calculate pagination
            offset = (page - 1) * limit
            end = min(offset + limit, total_count)
            
            # Get latest message for each conversation
            conversations = []
            for receiver_id, conversation_id in list(conversations_map.items())[offset:end]:
                # Get latest message
                message = cassandra_client.execute("""
                    SELECT * FROM conversations
                    WHERE conversation_id = %(conversation_id)s
                    ORDER BY created_at DESC, message_id DESC
                    LIMIT 1
                """, {'conversation_id': conversation_id})
                
                if message:
                    msg = message[0]
                    conversations.append(ConversationResponse(
                        id=conversation_id,
                        user1_id=user_id,
                        user2_id=receiver_id,
                        last_message_at=msg['created_at'],
                        last_message_content=msg['content']
                    ))
            
            return PaginatedConversationResponse(
                total=total_count,
                page=page,
                limit=limit,
                data=conversations
            )
            
        except Exception as e:
            raise Exception(f"Failed to get user conversations: {str(e)}")
    
    @staticmethod
    async def get_conversation(conversation_id: int) -> ConversationResponse:
        """
        Get a conversation by ID.
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Conversation data
        """
        try:
            # Get latest message from conversation
            message = cassandra_client.execute("""
                SELECT * FROM conversations
                WHERE conversation_id = %(conversation_id)s
                ORDER BY created_at DESC, message_id DESC
                LIMIT 1
            """, {'conversation_id': conversation_id})
            
            if not message:
                raise Exception("Conversation not found")
            
            msg = message[0]
            
            # Get participants from users table
            sender_data = cassandra_client.execute("""
                SELECT user_id, conversations FROM users
                WHERE conversations CONTAINS %(conversation_id)s
                LIMIT 1
            """, {'conversation_id': conversation_id})
            
            if not sender_data:
                raise Exception("Conversation participants not found")
            
            sender_id = sender_data[0]['user_id']
            conversations_map = sender_data[0]['conversations']
            
            # Find receiver_id from conversations map
            receiver_id = None
            for rid, cid in conversations_map.items():
                if cid == conversation_id:
                    receiver_id = rid
                    break
            
            if not receiver_id:
                raise Exception("Conversation receiver not found")
            
            return ConversationResponse(
                id=conversation_id,
                user1_id=sender_id,
                user2_id=receiver_id,
                last_message_at=msg['created_at'],
                last_message_content=msg['content']
            )
            
        except Exception as e:
            raise Exception(f"Failed to get conversation: {str(e)}")
    
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
        """
        try:
            # Get sender's conversations
            sender_data = cassandra_client.execute("""
                SELECT conversations FROM users
                WHERE user_id = %(user_id)s
            """, {'user_id': sender_id})
            
            if sender_data and sender_data[0]['conversations']:
                # Check if conversation exists with receiver
                conversation_id = sender_data[0]['conversations'].get(receiver_id)
                if conversation_id:
                    return conversation_id
            
            # Generate new conversation_id
            conversation_id = int(uuid.uuid4().int & ((1 << 31) - 1))
            
            # Update sender's conversations
            cassandra_client.execute("""
                UPDATE users
                SET conversations[%(receiver_id)s] = %(conversation_id)s
                WHERE user_id = %(sender_id)s
            """, {
                'sender_id': sender_id,
                'receiver_id': receiver_id,
                'conversation_id': conversation_id
            })
            
            # Update receiver's conversations
            cassandra_client.execute("""
                UPDATE users
                SET conversations[%(sender_id)s] = %(conversation_id)s
                WHERE user_id = %(receiver_id)s
            """, {
                'receiver_id': receiver_id,
                'sender_id': sender_id,
                'conversation_id': conversation_id
            })
            
            return conversation_id
            
        except Exception as e:
            raise Exception(f"Failed to create or get conversation: {str(e)}")