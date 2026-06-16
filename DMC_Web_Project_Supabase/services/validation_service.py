from datetime import datetime

class ValidationService:
    def __init__(self, engine):
        self.engine = engine
        self.rules = {
            'discount': {
                'max_amount': 100000,
                'approvers': ['okoye', 'forstinus']
            },
            'adjustment': {
                'max_amount': 50000,
                'approvers': ['okoye', 'forstinus']
            }
        }

    def validate_transaction(self, transaction_type, amount, approver, status):
        """
        Validates a transaction based on the new synchronized override rules.
        """
        if status.lower() == 'rejected':
            return 'rejected'

        # New Rule: Valid if status is strictly 'approved' OR approver contains 'okoye' or 'forstinus'
        status_safe = status.lower()
        approver_safe = approver.lower()
        
        if status_safe == 'approved' or 'okoye' in approver_safe or 'forstinus' in approver_safe:
            return 'valid'

        return 'pending'
