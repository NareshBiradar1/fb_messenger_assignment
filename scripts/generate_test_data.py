"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    
    Students should implement this function to generate test data based on their schema design.
    The function should create:
    - Users (with IDs 1-NUM_USERS)
    - Conversations between random pairs of users
    - Messages in each conversation with realistic timestamps
    """
    logger.info("Generating test data...")
    
    # Sample message content
    sample_messages = [
        "Hello!",
        "How are you?",
        "What's up?",
        "Nice to meet you!",
        "How's your day going?",
        "Any plans for the weekend?",
        "Did you see that movie?",
        "Let's catch up soon!",
        "Thanks for the message!",
        "I'll get back to you later."
    ]
    
    # Create users
    users = list(range(1, NUM_USERS + 1))
    logger.info(f"Created {len(users)} test users")
    
    # Create conversations
    conversations = []
    for _ in range(NUM_CONVERSATIONS):
        # Select random pair of users
        user1, user2 = random.sample(users, 2)
        
        # Generate conversation_id
        conversation_id = random.randint(1, 1000000)
        conversations.append((conversation_id, user1, user2))
        
        # Update both users' conversation maps
        session.execute("""
            UPDATE users
            SET conversations[%(other_user_id)s] = %(conversation_id)s
            WHERE user_id = %(user_id)s
        """, {
            'user_id': user1,
            'other_user_id': user2,
            'conversation_id': conversation_id
        })
        
        session.execute("""
            UPDATE users
            SET conversations[%(other_user_id)s] = %(conversation_id)s
            WHERE user_id = %(user_id)s
        """, {
            'user_id': user2,
            'other_user_id': user1,
            'conversation_id': conversation_id
        })
    
    # Create messages for each conversation
    for conversation_id, user1, user2 in conversations:
        # Generate random number of messages
        num_messages = random.randint(1, MAX_MESSAGES_PER_CONVERSATION)
        batch = BatchStatement()
        current_time = datetime.utcnow()
        
        for _ in range(num_messages):
            # Alternate between users as sender
            sender_id = user1 if _ % 2 == 0 else user2
            receiver_id = user2 if _ % 2 == 0 else user1
            
            # Generate message content
            content = random.choice(sample_messages)
            
            # Create message
            batch.add(SimpleStatement("""
                INSERT INTO conversations (
                    conversation_id, created_at, message_id,
                    sender_id, receiver_id, content
                ) VALUES (
                    %(conversation_id)s, %(created_at)s, now(),
                    %(sender_id)s, %(receiver_id)s, %(content)s
                )
            """), {
                'conversation_id': conversation_id,
                'created_at': current_time,
                'sender_id': sender_id,
                'receiver_id': receiver_id,
                'content': content
            })
            
            # Move time back for next message
            current_time -= timedelta(minutes=random.randint(1, 60))
        
        # Execute batch
        session.execute(batch)
        
        logger.info(
            f"Created conversation {conversation_id} between users "
            f"{user1} and {user2} with {num_messages} messages"
        )
    
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 