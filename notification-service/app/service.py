"""Notification Service Business Logic"""


class NotificationService:
    def send_notification(self, user_id: str, message: str):
        """Send notification to user"""
        print(f"Sending notification to {user_id}: {message}")
        # Implementation would send email/SMS/push notification
        return True
    
    def send_email(self, user_id: str, subject: str, body: str):
        """Send email notification"""
        print(f"Sending email to {user_id}: {subject}")
        return True
    
    def send_sms(self, user_id: str, message: str):
        """Send SMS notification"""
        print(f"Sending SMS to {user_id}: {message}")
        return True
