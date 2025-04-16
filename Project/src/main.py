from src.systems.mongodb_system import MongoDBSystem
from src.systems.hive_system import HiveSystem
from src.systems.mysql_system import MySQLSystem
from dotenv import load_dotenv

load_dotenv()  # Load .env file

def main():
    systems = {
        "MONGO": MongoDBSystem(),
        "HIVE": HiveSystem(),
        "SQL": MySQLSystem()
    }

    with open("tests/testcase.in", "r") as f:
        commands = f.readlines()

    for cmd in commands:
        cmd = cmd.strip()
        if not cmd:
            continue

        if cmd.startswith("MERGE"):
            systems_str = cmd[cmd.find("(") + 1 : cmd.find(")")]
            system1, system2 = map(str.strip, systems_str.split(","))
            system1= system1.strip().upper()
            system2= system2.strip().upper()
            

          
            if system1 in systems and system2 in systems:
                print(f"MERGING {system1} and {system2}")
                systems[system1].merge(system2)
            else:
                if system1 not in systems:
                    print(f"Invalid system 1: {system1}")
                if system2 not in systems:
                    print(f"Invalid system 2: {system2}")
            

        else:
            system_name, operation = cmd.split(".", 1)
            system_name = system_name.strip()
            operation = operation.strip()

            op_type = operation.split("(")[0]
            data = operation[operation.find("(")+1:operation.find(")")]
            parts = data.split(", ")
            admission_number = parts[0]
            subject = parts[1]
            period = parts[2]
            grade = parts[3] if len(parts) > 3 else None
            if system_name not in systems:
                print(f"Invalid system else: {system_name}")
                continue

            try:
                if op_type == "INSERT":
                    systems[system_name].insert(admission_number, subject, period, grade)
                    print(f"{system_name}.INSERT({admission_number}, {subject}, {period}, {grade})")
                elif op_type == "READ":
                    result = systems[system_name].read(admission_number, subject, period)
                    print(f"{system_name}.READ({admission_number}, {subject}, {period}) -> {result}")
                elif op_type == "UPDATE":
                    systems[system_name].update(admission_number, subject, period, grade)
                    print(f"{system_name}.UPDATE({admission_number}, {subject}, {period}, {grade})")
                elif op_type == "DELETE":
                    systems[system_name].delete(admission_number, subject, period)
                    print(f"{system_name}.DELETE({admission_number}, {subject}, {period})")
            except Exception as e:
                print(f"Error executing {system_name}.{op_type}: {e}")

if __name__ == "__main__":
    main()