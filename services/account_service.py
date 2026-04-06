from sqlalchemy import text
from datetime import datetime
from services.validation_service import ValidationService
from db_utils import is_online, get_local_engine # type: ignore

class AccountService:
    def __init__(self, engine, staff_repo, validation_service):
        self.engine = engine
        self.local_engine = get_local_engine()
        self.repo = staff_repo
        self.validation_service = validation_service

    def get_account_financials(self, account_number, username, role, force_online=False):
        """Reconstructed legacy financial logic from account_dashboard.py."""
        
        active_engine = self.engine if (force_online or is_online()) else self.local_engine
        
        with active_engine.connect() as conn:
            # 1. Fetch Customer/Demographic Info
            cust = conn.execute(text("SELECT * FROM customers WHERE account_number = :acc"), {"acc": account_number}).fetchone()
            
            if not cust:
                return None
            
            cust_dict = dict(cust._mapping)
            acc_num = str(account_number)
            
            # 2. Optimized Financial Fetching (Legacy Logic)
            fin_dict = {
                'account_number': acc_num,
                'total_debt': float(cust_dict.get('closing_balance', 0) or 0)
            }
            
            # Sum of POS payments (from collections)
            pos_res = conn.execute(text("SELECT SUM(amount_paid) FROM collections WHERE account_number = :acc"), {"acc": acc_num}).scalar()
            fin_dict['pos_payments'] = float(pos_res) if pos_res else 0.0

            # Sum of Other payments
            oth_res = conn.execute(text("SELECT SUM(amount_paid) FROM other_payments WHERE account_number = :acc"), {"acc": acc_num}).scalar()
            fin_dict['other_payments'] = float(oth_res) if oth_res else 0.0

            # Total Payments (For Payment Plan/Debt logic)
            p_res = conn.execute(text("SELECT SUM(amount_paid) FROM all_payments WHERE account_number = :acc"), {"acc": acc_num}).scalar()
            fin_dict['total_payments'] = float(p_res) if p_res else 0.0
            
            # Discounts - Detailed Fetching (Legacy Logic)
            d_all = conn.execute(text("SELECT status, discounted_amount, user_who_approved FROM discounts WHERE account_number = :acc"), {"acc": acc_num}).fetchall()
            fin_dict['total_discounts_valid'] = 0.0
            fin_dict['total_discounts_pending'] = 0.0
            fin_dict['total_discounts_rejected'] = 0.0
            fin_dict['discounts'] = []
            
            for d in d_all:
                amt = float(d[1]) if d[1] else 0.0
                status = str(d[0]).lower() if d[0] else ""
                approver = str(d[2]).lower() if d[2] else ""
                
                validation_status = self.validation_service.validate_transaction('discount', amt, approver, status)
                fin_dict['discounts'].append({'amount': amt, 'status': validation_status, 'approver': approver})

                if validation_status == 'valid':
                    fin_dict['total_discounts_valid'] += amt
                elif validation_status == 'rejected':
                    fin_dict['total_discounts_rejected'] += amt
                else:
                    fin_dict['total_discounts_pending'] += amt

            # Adjustments - Detailed Fetching (Legacy Logic)
            a_all = conn.execute(text("SELECT status, adjustment_amount, user_who_approved_adjustment FROM adjustments WHERE account_number = :acc"), {"acc": acc_num}).fetchall()
            fin_dict['total_adjustments_valid'] = 0.0
            fin_dict['total_adjustments_pending'] = 0.0
            fin_dict['total_adjustments_rejected'] = 0.0
            fin_dict['adjustments'] = []
            
            for a in a_all:
                amt = float(a[1]) if a[1] else 0.0
                status = str(a[0]).lower() if a[0] else ""
                approver = str(a[2]).lower() if a[2] else ""
                
                validation_status = self.validation_service.validate_transaction('adjustment', amt, approver, status)
                fin_dict['adjustments'].append({'amount': amt, 'status': validation_status, 'approver': approver})

                if validation_status == 'valid':
                    fin_dict['total_adjustments_valid'] += amt
                elif validation_status == 'rejected':
                    fin_dict['total_adjustments_rejected'] += amt
                else:
                    fin_dict['total_adjustments_pending'] += amt
            
            # Calculated Outstanding (ONLY valid/approved ones are used)
            fin_dict['outstanding_balance'] = fin_dict['total_debt'] - fin_dict['total_payments'] - fin_dict['total_discounts_valid'] - fin_dict['total_adjustments_valid']
            
            # Payment Plan logic (30% rule)
            if (fin_dict['total_payments'] >= 0.3 * fin_dict['total_debt']) and (fin_dict['outstanding_balance'] > 0):
                fin_dict['payment_plan'] = 'Yes'
            else:
                fin_dict['payment_plan'] = 'No'
            
            # 3. Fetch Validation Details
            val = conn.execute(text("SELECT * FROM validation WHERE account_number = :acc ORDER BY validation_date DESC LIMIT 1"), {"acc": acc_num}).fetchone()
            validation = dict(val._mapping) if val else None
            
            # 4. Fetch Resolution Info
            res = conn.execute(text("SELECT * FROM resolutions WHERE account_number = :acc LIMIT 1"), {"acc": acc_num}).fetchone()
            resolution = dict(res._mapping) if res else None
            
            # 5. Fetch All Payments (Unified)
            payments = conn.execute(text("SELECT date_of_payment, amount_paid, payment_source FROM all_payments WHERE account_number = :acc ORDER BY date_of_payment DESC"), {"acc": acc_num}).fetchall()
            history = [{"date": p[0], "amount": float(p[1]), "source": p[2]} for p in payments]

            return {
                "account": cust_dict,
                "financials": fin_dict,
                "validation": validation,
                "resolution": resolution,
                "history": history
            }
