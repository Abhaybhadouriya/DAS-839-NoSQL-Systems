# from src.systems.mongodb_system import MongoDBSystem
# from src.systems.hive_system import HiveSystem
# from src.systems.mysql_system import MySQLSystem
# from dotenv import load_dotenv

# load_dotenv()  # Load .env file

# def main():
#     systems = {
#         "MONGO": MongoDBSystem(),
#         "HIVE": HiveSystem(),
#         "SQL": MySQLSystem()
#     }

#     with open("tests/testcase.in", "r") as f:
#         commands = f.readlines()

#     for cmd in commands:
#         cmd = cmd.strip()
#         if not cmd:
#             continue

#         if cmd.startswith("MERGE"):
#             systems_str = cmd[cmd.find("(") + 1 : cmd.find(")")]
#             system1, system2 = map(str.strip, systems_str.split(","))
#             system1= system1.strip().upper()
#             system2= system2.strip().upper()
            

          
#             if system1 in systems and system2 in systems:
#                 print(f"MERGING {system1} and {system2}")
#                 systems[system1].merge(system2)
#             else:
#                 if system1 not in systems:
#                     print(f"Invalid system 1: {system1}")
#                 if system2 not in systems:
#                     print(f"Invalid system 2: {system2}")
            

#         else:
#             system_name, operation = cmd.split(".", 1)
#             system_name = system_name.strip()
#             operation = operation.strip()

#             op_type = operation.split("(")[0]
#             data = operation[operation.find("(")+1:operation.find(")")]
#             parts = data.split(", ")
#             admission_number = parts[0]
#             subject = parts[1]
#             period = parts[2]
#             grade = parts[3] if len(parts) > 3 else None
#             if system_name not in systems:
#                 print(f"Invalid system else: {system_name}")
#                 continue

#             try:
#                 if op_type == "INSERT":
#                     systems[system_name].insert(admission_number, subject, period, grade)
#                     print(f"{system_name}.INSERT({admission_number}, {subject}, {period}, {grade})")
#                 elif op_type == "READ":
#                     result = systems[system_name].read(admission_number, subject, period)
#                     print(f"{system_name}.READ({admission_number}, {subject}, {period}) -> {result}")
#                 elif op_type == "UPDATE":
#                     systems[system_name].update(admission_number, subject, period, grade)
#                     print(f"{system_name}.UPDATE({admission_number}, {subject}, {period}, {grade})")
#                 elif op_type == "DELETE":
#                     systems[system_name].delete(admission_number, subject, period)
#                     print(f"{system_name}.DELETE({admission_number}, {subject}, {period})")
#             except Exception as e:
#                 print(f"Error executing {system_name}.{op_type}: {e}")

# if __name__ == "__main__":
#     main()



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
    choiceInputFile = input("Enter the input file name (default: 1  =  tests/testcase_1.in) else \n1. choose 1 = tests/testcase_1.in)\n2. choose 2 = tests/testcase_2.in)\n3. choose 3 = tests/testcase_3.in)\n4. choose 4 = tests/testcase_4.in)\n5. choose 5 = tests/testcase_5.in)\n6. choose 6 = tests/testcase_6.in)\n7. choose 7 = tests/testcase_7.in)\n8. choose 8 = tests/testcase_8.in)\n9. choose 9 = tests/testcase_9.in)\n10. choose 10 = tests/testcase_10.in)\n")
    if not choiceInputFile:
        choiceInputFile = "tests/testcase.in"
    print(f"Using input file: {choiceInputFile}")
    match choiceInputFile:
        case "1":
            choiceInputFile = "tests/testcase_1.in"
        case "2":
            choiceInputFile = "tests/testcase_2.in"
        case "3":
            choiceInputFile = "tests/testcase_3.in"
        case "4":
            choiceInputFile = "tests/testcase_4.in"
        case "5":
            choiceInputFile = "tests/testcase_5.in"
        case "6":
            choiceInputFile = "tests/testcase_6.in"
        case "7":
            choiceInputFile = "tests/testcase_7.in"
        case "8":
            choiceInputFile = "tests/testcase_8.in"
        case "9":
            choiceInputFile = "tests/testcase_9.in"
        case "10":
            choiceInputFile = "tests/testcase_10.in"
        case _:
            print("Invalid choice, using default input file: tests/testcase.in")
            choiceInputFile = "tests/testcase.in"
    # Read commands from the input file
    with open(choiceInputFile, "r") as f:
        commands = f.readlines()

    for cmd in commands:
        cmd = cmd.strip()
        if not cmd:
            continue

        if cmd.startswith("HIVE") or cmd.startswith("MONGO") or cmd.startswith("SQL"):
            system_name, operation = cmd.split(".", 1)
            system_name = system_name.strip()
            operation = operation.strip()
            op_type = operation.split("(")[0]
            start_idx = operation.find("(") + 1
            end_idx = operation.find(")")
            if start_idx == 0 or end_idx == -1:
                print(f"Invalid operation format: {operation}")
                continue
            data = operation[start_idx:end_idx].strip()
            system1 = system_name.strip().upper()
            system2 = data.strip().upper()
            print(f"System 1: {system1}, System 2: {system2}")
            if system1 in systems and system2 in systems:
                print(f"MERGING {system1} and {system2}")
                systems[system1].merge(system2)
            else:
                if system1 not in systems:
                    print(f"Invalid system 1: {system1}")
                if system2 not in systems:
                    print(f"Invalid system 2: {system2}")
            
        else:
            try:
                log_number,operation = cmd.split(",",1)
                log_number = log_number.strip()
                operation = operation.strip()
                if not log_number.isdigit():
                    print(f"Invalid log number: {log_number}")
                    continue
                log_number = int(log_number)
                if log_number < 0:
                    print(f"Log number must be non-negative: {log_number}")
                    continue
                system_name ,operation= operation.split(".")
                system_name = system_name.strip()
                operation = operation.strip()
                if system_name not in systems:
                    print(f"Invalid system: {system_name}")
                    continue
                op_type = operation.split("(")[0]
                if op_type not in ["SET", "GET", "DELETE"]:
                    print(f"Invalid operation type: {op_type}")
                    continue

                start_idx = operation.find("(") + 1
                end_idx = operation.find(")")
                if start_idx == 0 or end_idx == -1:
                    print(f"Invalid operation format: {operation}")
                    continue
                data = operation[start_idx:end_idx].strip()
                parts = [part.strip() for part in data.split(",")]
                print(parts)
                if len(parts) < 2:
                    print(f"Insufficient parameters in {cmd}")
                    continue

                sid = parts[0]
                course = parts[1]
                grade = parts[2] if len(parts) > 2 else None

                if system_name not in systems:
                    print(f"Invalid system else: {system_name}")
                    continue

                if op_type == "SET":
                    if not grade:
                        print(f"Grade required for SET operation in {cmd}")
                        continue
                    systems[system_name].insert(sid, course, grade)
                    print(f"{system_name}.SET({sid}, {course}, {grade})")
                elif op_type == "GET":
                    result = systems[system_name].read(sid, course)
                    print(f"{system_name}.GET({sid}, {course}) -> {result}")
                elif op_type == "DELETE":
                    systems[system_name].delete(sid, course)
                    print(f"{system_name}.DELETE({sid}, {course})")
            except Exception as e:
                print(f"Error executing {system_name}.{op_type}: {e}")

if __name__ == "__main__":
    main()