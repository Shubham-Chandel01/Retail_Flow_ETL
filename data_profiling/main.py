from profiler import generate_profile_report
from config.config import load_config
import logging

def main():
    try:
        config = load_config()
        input_file = config['retail_data_file_path']
        output_file = config["retail_data_profile_path"]
        # Generate the profiling report
        generate_profile_report(input_file, output_file)

    except Exception as e:
        logging.error(f"An error occurred while generation of profile report : {e}")


if __name__ == "__main__":
    main()