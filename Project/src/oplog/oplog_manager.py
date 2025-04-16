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
                f.write(f"{op_id}, {operation} ({key_str}, {grade}, {timestamp})\n")
            elif operation in ["INSERT", "UPDATE"]:
                f.write(f"{op_id}, {operation} ({key_str}, {grade}, {timestamp})\n")

    def read_log(self):
        """Read and parse the operation log."""
        operations = []
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(", ")
                    if len(parts) < 2:
                        continue  # Skip invalid lines
                    
                    op_id = int(parts[0])
                    op_type = parts[1].split(" ")[0]
                    
                    # Safely parse the (admission_number, subject, period)
                    full_args = parts[1]
                    start = full_args.find("(") + 1
                    end = full_args.rfind(")")
                    if end == -1:  # If ')' is missing, use till end of string
                        end = len(full_args)
                    
                    try:
                        admission_number, subject, period = map(str.strip, full_args[start:end].split(","))
                    except ValueError:
                        print(f"Malformed line (args): {line.strip()}")
                        continue  # Skip lines with bad format
                    
                    # Optional fields
                    grade = parts[2].strip() if len(parts) > 2 else None
                    timestamp = None
                    if len(parts) > 3:
                        timestamp = parts[3].strip()
                        if timestamp.endswith(")"):
                            timestamp = timestamp[:-1]                    
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
