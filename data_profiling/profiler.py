import pandas as pd
from ydata_profiling import ProfileReport


# Generates a data profiling report for the input CSV file.
def generate_profile_report(input_file, output_file):
    df = pd.read_csv(input_file)

    # Generate the profiling report
    profile = ProfileReport(df, title="Retail Sales Profiling Report", explorative=True)

    profile.to_file(output_file)