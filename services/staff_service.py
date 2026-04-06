from repositories.staff_repo import StaffRepository

class StaffService:
    def __init__(self, staff_repo: StaffRepository):
        self.repo = staff_repo

    def get_all_staff(self):
        return self.repo.get_all_staff()

    def add_staff(self, username, password, full_name, role):
        # In a real app, you'd hash the password here
        return self.repo.add_staff(username, password, full_name, role)

    def delete_staff(self, staff_id):
        return self.repo.delete_staff(staff_id)

    def get_activity_log(self, **filters):
        return self.repo.get_activity_log(**filters)
