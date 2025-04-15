import time
from datetime import datetime

class OpLogManager:
    def __init__(self, log_file):
        self.log_file = log_file

    def log_operation(self, op_id, operation, key_tuple, grade=None, timestamp=None):
        """Append operation to the log file with an optional timestamp."""
        admission_number, subject, period = key_tuple
        key_str = f"{admission_number},{subject},{period}"
        timestamp = timestamp or datetime.now().isoformat()
        with open(self.log_file, 'a') as f:
            if operation == "READ" or operation == "DELETE":
                f.write(f"{op_id}, {operation} ({key_str}, {timestamp})\n")
            elif operation in ["INSERT", "UPDATE"]:
                f.write(f"{op_id}, {operation} ({key_str}, {grade}, {timestamp})\n")

    def read_log(self):
        """Read and parse the operation log."""
        operations = []
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(", ")
                    op_id = int(parts[0])
                    op_type = parts[1].split(" ")[0]
                    data = parts[1][parts[1].find("(")+1:parts[1].find(")")]
                    if "," in data and " " in data:
                        key_part, rest = data.rsplit(", ", 1)
                        admission_number, subject, period = key_part.split(",")
                        if rest:
                            if ", " in rest:
                                grade, timestamp = rest.rsplit(", ", 1)
                            else:
                                grade, timestamp = rest, None
                        else:
                            grade, timestamp = None, None
                    else:
                        admission_number, subject, period = data.split(",")
                        grade, timestamp = None, None
                    operations.append({
                        "op_id": op_id,
                        "operation": op_type,
                        "admission_number": admission_number,
                        "subject": subject,
                        "period": period,
                        "grade": grade,
                        "timestamp": timestamp
                    })
        except FileNotFoundError:
            pass
        return sorted(operations, key=lambda x: (x["op_id"], x["timestamp"] or "1970-01-01T00:00:00"))
