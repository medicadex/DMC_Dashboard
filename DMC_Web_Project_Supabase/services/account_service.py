from sqlalchemy import text
from datetime import datetime
from services.validation_service import ValidationService

class AccountService:
    def __init__(self, engine, staff_repo, validation_service):
        self.engine = engine
        self.repo = staff_repo
        self.validation_service = validation_service

    def get_account_financials(self, account_number, username, role, force_online=False):
        """Reconstructed legacy financial logic from account_dashboard.py."""
        
        with self.engine.connect() as conn:
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

            # 2. Payments (all_payments table)
            # Fetch total payments and last payment date
            p_res = conn.execute(text("SELECT SUM(amount_paid), MAX(date_of_payment) FROM all_payments WHERE account_number = :acc"), {"acc": acc_num}).fetchone()
            fin_dict['total_payments'] = float(p_res[0]) if p_res[0] else 0.0
            fin_dict['last_payment_date'] = p_res[1] # datetime or None
            
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
                
                # Payment Plan Status (Active/Defaulted)
                last_pay = fin_dict.get('last_payment_date')
                if not last_pay:
                    fin_dict['payment_plan_status'] = "Defaulted"
                else:
                    # Parse if string (SQLite) or use directly if datetime (MySQL)
                    if isinstance(last_pay, str):
                        try: last_pay = datetime.strptime(last_pay[:19], '%Y-%m-%d %H:%M:%S')
                        except: last_pay = None
                    
                    if not last_pay:
                        fin_dict['payment_plan_status'] = "Defaulted"
                    else:
                        # Normalize both to date objects to avoid "datetime.datetime vs datetime.date" error
                        last_pay_date = last_pay.date() if isinstance(last_pay, datetime) else last_pay
                        diff = (datetime.now().date() - last_pay_date).days
                        fin_dict['payment_plan_status'] = "Active" if diff <= 30 else "Defaulted"
            else:
                fin_dict['payment_plan'] = 'No'
                fin_dict['payment_plan_status'] = "No Plan"
            
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

    def update_officer_start_dates_batch(self, updates):
        """
        Updates start dates for a batch of account officer shifts.
        'updates' is a list of dicts: [{'account_number': '...', 'start_date': 'YYYY-MM-DD'}, ...]
        """
        success_count = 0
        with self.engine.begin() as conn:
            for item in updates:
                acc = item.get('account_number')
                new_date = item.get('start_date')
                if not acc or not new_date:
                    continue
                    
                # 1. Find the current history record
                current_history = conn.execute(
                    text("SELECT id FROM customer_officer_history WHERE account_number = :acc AND is_current = 1"),
                    {"acc": acc}
                ).fetchone()

                if not current_history:
                    continue

                current_id = current_history[0]

                # 2. Update the current record's start_date
                conn.execute(
                    text("UPDATE customer_officer_history SET start_date = :new_date WHERE id = :id"),
                    {"new_date": new_date, "id": current_id}
                )

                # 3. Update the previous record's end_date
                prev_history = conn.execute(
                    text("SELECT id FROM customer_officer_history WHERE account_number = :acc AND id < :curr_id ORDER BY id DESC LIMIT 1"),
                    {"acc": acc, "curr_id": current_id}
                ).fetchone()

                if prev_history:
                    prev_id = prev_history[0]
                    conn.execute(
                        text("UPDATE customer_officer_history SET end_date = :new_date WHERE id = :id"),
                        {"new_date": new_date, "id": prev_id}
                    )
                success_count += 1
                
        return True, f"Successfully updated {success_count} account officer start dates in batch."

    def update_officer_start_date(self, account_number, new_start_date):
        """Updates the start date of the current officer and the end date of the previous one."""
        with self.engine.begin() as conn:
            # 1. Find the current history record
            current_history = conn.execute(
                text("SELECT id FROM customer_officer_history WHERE account_number = :acc AND is_current = 1"),
                {"acc": account_number}
            ).fetchone()

            if not current_history:
                return False, "No active history record found for this account."

            current_id = current_history[0]

            # 2. Update the current record's start_date
            conn.execute(
                text("UPDATE customer_officer_history SET start_date = :new_date WHERE id = :id"),
                {"new_date": new_start_date, "id": current_id}
            )

            # 3. Update the previous record's end_date
            prev_history = conn.execute(
                text("SELECT id FROM customer_officer_history WHERE account_number = :acc AND id < :curr_id ORDER BY id DESC LIMIT 1"),
                {"acc": account_number, "curr_id": current_id}
            ).fetchone()

            if prev_history:
                prev_id = prev_history[0]
                conn.execute(
                    text("UPDATE customer_officer_history SET end_date = :new_date WHERE id = :id"),
                    {"new_date": new_start_date, "id": prev_id}
                )

            return True, "Start date updated successfully."

    def detect_officer_changes_in_df(self, df_uploaded):
        """Compares uploaded dataframe with existing database to detect account officer changes."""
        import pandas as pd
        if 'account_number' not in df_uploaded.columns or 'account_officer' not in df_uploaded.columns:
            return {}
            
        acc_list = df_uploaded['account_number'].dropna().unique().tolist()
        if not acc_list:
            return {}
            
        existing_officers = {}
        try:
            with self.engine.connect() as conn:
                chunk_size = 900
                for i in range(0, len(acc_list), chunk_size):
                    chunk = acc_list[i:i+chunk_size]
                    placeholders = ", ".join([f":acc{idx}" for idx in range(len(chunk))])
                    params = {f"acc{idx}": acc for idx, acc in enumerate(chunk)}
                    
                    query = f"SELECT account_number, account_officer FROM customers WHERE account_number IN ({placeholders})"
                    res = conn.execute(text(query), params).fetchall()
                    for acc, officer in res:
                        existing_officers[str(acc).strip()] = str(officer).strip() if officer else ""
        except Exception as e:
            import logging
            logging.error(f"Error querying existing officers: {e}")
            return {}
            
        changes = {}
        for _, row in df_uploaded.iterrows():
            acc = row.get('account_number')
            new_officer = row.get('account_officer')
            if pd.isnull(acc) or pd.isnull(new_officer):
                continue
                
            acc_str = str(acc).strip()
            new_officer_str = str(new_officer).strip()
            
            if acc_str in existing_officers:
                old_officer_str = existing_officers[acc_str]
                if old_officer_str.lower() != new_officer_str.lower() and new_officer_str:
                    if new_officer_str not in changes:
                        changes[new_officer_str] = []
                    changes[new_officer_str].append(acc_str)
                    
        return changes

