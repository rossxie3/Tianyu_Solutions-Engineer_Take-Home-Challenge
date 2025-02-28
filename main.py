import subprocess
import sys
import os

def run_script(script_path):
    """
    Execute a single script and output its results.

    This function does the following:
      - Checks if the script exists. If not, it prints an error message and exits.
      - Runs the script using Python and captures the output.
      - Prints the standard output from the script.
      - If the script fails (i.e., returns a non-zero exit code), it prints the error message and exits.
      - Otherwise, it prints a success message.
    """
    if not os.path.exists(script_path):
        print(f"Script {script_path} does not exist!")
        sys.exit(1)

    print(f"\n===== Running {script_path} =====")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error executing script {script_path}:")
        print(result.stderr)
        sys.exit(result.returncode)
    else:
        print(f"===== {script_path} executed successfully =====\n")

def main():
    # List the scripts to run in sequence according to our data processing workflow:
    # 1. Parse raw JSON data and generate raw CSV files.
    # 2. Clean user data.
    # 3. Clean brand data.
    # 4. Clean receipts data.
    # 5. Parse receipt items from receipts.
    # 6. Load the CSV data into an SQLite database.
    # 7. Execute SQL queries to validate and analyze the results.
    scripts = [
        "import json.py",
        "clean_users.py",
        "clean_brands.py",
        "clean_receipts.py",
        "extract_receipt_items.py",
        "load_to_sql.py",
        "check_data_quality.py",
        "query_sql.py"
    ]

    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    main()
