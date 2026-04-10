# utils/password_helper.py
import base64
import hashlib
import logging
import os
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from config.database import execute_query, execute_non_query, get_db
import bcrypt

logger = logging.getLogger(__name__)

# Database column mappings for ComUsers table based on your actual schema
COLUMN_MAPPING = {
    'user_id': 'Uid',
    'username': 'Userlogin',
    'password': 'Userpassword',
    'token': 'FBRTokenNo',
    'is_active': 'IsActive',
    'fullname': 'Userfullname',
    'email': 'Useremail',
    'mobile': 'userMobile'
}

# Try to get encryption key from environment or use default
ENCRYPTION_KEY = os.environ.get('ENCRYPTION_KEY', 'AwaisFancy@2024SecretKeyForEncryption!')
ENCRYPTION_IV = os.environ.get('ENCRYPTION_IV', 'FancyERP2024IV')


def prepare_key(key_string):
    """Prepare key to be exactly 32 bytes for AES-256"""
    key_bytes = key_string.encode('utf-8')
    if len(key_bytes) < 32:
        key_bytes = key_bytes.ljust(32, b'0')
    elif len(key_bytes) > 32:
        key_bytes = key_bytes[:32]
    return key_bytes

def prepare_iv(iv_string):
    """Prepare IV to be exactly 16 bytes for AES-CBC"""
    iv_bytes = iv_string.encode('utf-8')
    if len(iv_bytes) < 16:
        iv_bytes = iv_bytes.ljust(16, b'0')
    elif len(iv_bytes) > 16:
        iv_bytes = iv_bytes[:16]
    return iv_bytes

def encrypt_password(password):
    """Encrypt password using AES-256-CBC"""
    try:
        if not password:
            return None
        
        key = prepare_key(ENCRYPTION_KEY)
        iv = prepare_iv(ENCRYPTION_IV)
        
        # Create cipher
        cipher = AES.new(key, AES.MODE_CBC, iv)
        
        # Pad password to multiple of 16 bytes
        padded_password = pad(password.encode('utf-8'), AES.block_size)
        
        # Encrypt
        encrypted = cipher.encrypt(padded_password)
        
        # Return as base64 string
        return base64.b64encode(encrypted).decode('utf-8')
        
    except Exception as e:
        logger.error(f"Encryption error: {e}")
        return None

def decrypt_password(encrypted_password):
    """Decrypt password using AES-256-CBC"""
    try:
        if not encrypted_password:
            return None
        
        key = prepare_key(ENCRYPTION_KEY)
        iv = prepare_iv(ENCRYPTION_IV)
        
        # Decode from base64
        encrypted_data = base64.b64decode(encrypted_password)
        
        # Create cipher
        cipher = AES.new(key, AES.MODE_CBC, iv)
        
        # Decrypt
        decrypted_padded = cipher.decrypt(encrypted_data)
        
        # Unpad
        decrypted = unpad(decrypted_padded, AES.block_size)
        
        return decrypted.decode('utf-8')
        
    except Exception as e:
        logger.error(f"Decryption error: {e}")
        return None

def hash_password_bcrypt(password):
    """Hash a password using bcrypt"""
    try:
        if not password:
            return None
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')
    except Exception as e:
        logger.error(f"Error hashing password with bcrypt: {e}")
        return None

def hash_password_sha256(password):
    """Hash password using SHA256 (legacy fallback)"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(plain_password, stored_password):
    """Verify if plain password matches stored password"""
    try:
        if not plain_password or not stored_password:
            return False
        
        # First, try bcrypt verification (modern format)
        if stored_password.startswith('$2b$'):
            try:
                if bcrypt.checkpw(plain_password.encode('utf-8'), stored_password.encode('utf-8')):
                    logger.info("Password matched using bcrypt")
                    return True
            except Exception as e:
                logger.warning(f"bcrypt verification failed: {e}")
        
        # Try direct comparison (plain text)
        if plain_password == stored_password:
            logger.info("Password matched as plain text")
            # Upgrade to bcrypt for future verifications
            upgrade_to_bcrypt(plain_password, stored_password)
            return True
        
        # Try SHA256 hash comparison
        if hash_password_sha256(plain_password) == stored_password:
            logger.info("Password matched using SHA256")
            # Upgrade to bcrypt
            upgrade_to_bcrypt(plain_password, stored_password)
            return True
        
        # Try to decrypt the stored password (legacy AES encryption)
        try:
            decrypted = decrypt_password(stored_password)
            if decrypted and plain_password == decrypted:
                logger.info("Password matched after decryption")
                # Upgrade to bcrypt
                upgrade_to_bcrypt(plain_password, stored_password)
                return True
        except Exception as e:
            logger.warning(f"Decryption attempt failed: {e}")
        
        return False
        
    except Exception as e:
        logger.error(f"Password verification error: {e}")
        return False

def upgrade_to_bcrypt(plain_password, current_password):
    """Upgrade legacy password to bcrypt hash"""
    try:
        # Find user by current password
        query = """
            SELECT Uid FROM ComUsers 
            WHERE Userpassword = ?
        """
        results = execute_query(query, [current_password])
        
        if results:
            bcrypt_hash = hash_password_bcrypt(plain_password)
            if bcrypt_hash:
                # Only update Userpassword, no EncryptedPassword column
                update_query = """
                    UPDATE ComUsers 
                    SET Userpassword = ?
                    WHERE Uid = ?
                """
                execute_non_query(update_query, [bcrypt_hash, results[0]['Uid']])
                logger.info("Password upgraded to bcrypt")
                return True
    except Exception as e:
        logger.error(f"Error upgrading password to bcrypt: {e}")
    return False

def get_user_credentials(username):
    """Get user credentials from database"""
    try:
        query = """
            SELECT 
                Uid,
                Userlogin,
                Userpassword
            FROM ComUsers 
            WHERE Userlogin = ? AND IsActive = 1
        """
        
        logger.info(f"Executing query for user: {username}")
        results = execute_query(query, [username])
        
        if results and len(results) > 0:
            logger.info(f"User found: {username}")
            return {
                'uid': results[0]['Uid'],
                'username': results[0]['Userlogin'],
                'encrypted_password': results[0]['Userpassword']
            }
        
        logger.warning(f"User not found: {username}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting user credentials: {e}")
        return None

def update_user_token(username, token):
    """Update user token in database"""
    try:
        query = """
            UPDATE ComUsers 
            SET FBRTokenNo = ?
            WHERE Userlogin = ?
        """
        
        rows_affected = execute_non_query(query, [token, username])
        
        if rows_affected > 0:
            logger.info(f"Token updated for user: {username}")
            return True, None
        else:
            logger.warning(f"User not found for token update: {username}")
            return False, "User not found"
        
    except Exception as e:
        logger.error(f"Error updating user token: {e}")
        return False, str(e)

def create_user(username, plain_password, user_fullname=None, email=None, mobile=None):
    """Create a new user with bcrypt hashed password"""
    try:
        # Hash password using bcrypt
        hashed_password = hash_password_bcrypt(plain_password)
        
        if not hashed_password:
            hashed_password = plain_password  # fallback to plain text
        
        query = """
            INSERT INTO ComUsers (
                Userlogin, 
                Userpassword,
                Userfullname,
                Useremail,
                userMobile,
                IsActive
            ) VALUES (?, ?, ?, ?, ?, 1)
        """
        
        params = [username, hashed_password, user_fullname, email, mobile]
        rows_affected = execute_non_query(query, params)
        
        if rows_affected > 0:
            logger.info(f"User created: {username}")
            return True, None
        else:
            return False, "Failed to create user"
            
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        return False, str(e)

def update_password(username, new_password):
    """Update user password with bcrypt hashing - ONLY uses Userpassword column"""
    try:
        # Hash new password using bcrypt
        hashed_password = hash_password_bcrypt(new_password)
        
        if not hashed_password:
            hashed_password = new_password  # fallback to plain text
        
        # Only update Userpassword column (no EncryptedPassword)
        query = """
            UPDATE ComUsers 
            SET Userpassword = ?,
                FBRTokenNo = NULL
            WHERE Userlogin = ?
        """
        
        rows_affected = execute_non_query(query, [hashed_password, username])
        
        if rows_affected > 0:
            logger.info(f"Password updated for user: {username}")
            return True, None
        else:
            return False, "User not found"
            
    except Exception as e:
        logger.error(f"Error updating password: {e}")
        return False, str(e)

def validate_password_strength(password):
    """Validate password strength"""
    if len(password) < 3:
        return False, "Password must be at least 3 characters long"
    return True, "Password is valid"

# Legacy function names for backward compatibility
def hash_password(password):
    """Legacy function - use hash_password_bcrypt instead"""
    return hash_password_bcrypt(password)