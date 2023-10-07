"""
This program will verify election results for the Senate by cross-referencing votes with student data and determine the elected candidates.

You will need two csv files to run this program:
    1. A csv file with student data called 'data.csv'
    2. A csv file with the voting responses that will be identified automatically

You will need to import modules in the terminal using the requirements.txt file such as:
    pip install -r requirements.txt

You can adjust the variables in the config.ini file to match the election. The school should be either "ses", "sob", "sse", or "hass".

Author: Rocco Vaccone
Date: 10/1/2023
Updated: 10/7/2023
"""

# Module imports
import pandas as pd
from tqdm import tqdm
from collections import defaultdict
import configparser
import sys
import logging
from datetime import date
import os

# File imports
from dict import school_majors


# Retrieve the values from the config.ini file
def load_config():
    """
    Retrieves the configurations from the config.ini file, processes them and returns them as a dictionary.

    Returns:
    config_dict (dict): A dictionary where each key-value pair is a configuration item from the config.ini file.
    """
    config = configparser.ConfigParser()
    config.read("config.ini")
    config_dict = {k.lower(): v.strip('"') for k, v in config["DEFAULT"].items()}
    config_dict["num_seats"] = int(config_dict["num_seats"])
    return config_dict


# Identify one csv file in the directory
def find_csv_file(excluded_files=["data"]):
    """
    Identifies one csv file in the directory and returns the name of the file.

    Args:
    excluded_files (list): A list of file names that should be excluded from the search.

    Returns:
    file (str): The name of the csv file.
    """
    files = [
        file
        for file in os.listdir()
        if os.path.isfile(file)
        and file.endswith(".csv")
        and file.replace(".csv", "") not in excluded_files
    ]
    if not files:
        raise FileNotFoundError("CSV file not found")
    if len(files) != 1:
        raise FileExistsError("Multiple CSV files found")
    return files[0]


# Load the data from the csv files
def load_data(data_file, voting_file):
    """
    Loads the data from the csv files and returns them as DataFrames.

    Args:
    data_file (str): The name of the csv file with the student data.
    voting_file (str): The name of the csv file with the voting responses.

    Returns:
    data_df (DataFrame): A DataFrame with the student data.
    votes_df (DataFrame): A DataFrame with the voting responses.
    """
    try:
        data_df = pd.read_csv(data_file)
        votes_df = pd.read_csv(voting_file)
    except FileNotFoundError as e:
        print(e)
        sys.exit(e.errno)
    return data_df, votes_df


# Create the logger
def setup_logger():
    """
    Sets up the logger to log warnings and errors to a file.

    Returns:
    None
    """
    today = date.today().strftime("%m-%d-%Y")
    logging.basicConfig(
        level=logging.INFO,
        filename=f"InvalidAndDuplicateVotes_{today}.log",
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s\n",
    )


# Initialize the votes, voting record, and voter CWIDs
def initialize_setups():
    """
    Initializes the votes, voting record, and voter CWIDs.

    Returns:
    votes (dict): A dictionary where each key-value pair is a candidate and the number of votes they received.
    voting_record (dict): A dictionary where each key-value pair is a type of vote and the number of votes of that type.
    voter_cwids (set): A set of CWIDs of the voters.
    """
    votes = {}
    voting_record = {
        "valid": 0,
        "invalid": 0,
        "wrong_school": 0,
        "duplicate": 0,
    }
    voter_cwids = set()
    return votes, voting_record, voter_cwids


# Add votes from a list to the votes dictionary
def add_votes(candidate_list, votes, voting_record):
    """
    Adds votes from a list to the votes dictionary.

    Args:
    candidate_list (list): A list of candidates.
    votes (dict): A dictionary where each key-value pair is a candidate and the number of votes they received.
    voting_record (dict): A dictionary where each key-value pair is a type of vote and the number of votes of that type.

    Returns:
    None
    """
    for candidate in candidate_list:
        voting_record["valid"] += 1
        if candidate not in votes.keys():
            votes[candidate] = 1
        else:
            votes[candidate] += 1


# Determine the school of a major
def school_by_major(major, data_dict):
    """
    Determines the school of a major using a dictionary of schools and majors.

    Args:
    major (str): The major to find the school of.
    data_dict (dict): A dictionary where each key-value pair is a school and a list of majors.

    Returns:
    school (str): The school of the major.
    """
    if major is None:
        return None
    major = major.lower()
    for school, majors in data_dict.items():
        if major in majors:
            return school
    return None


# Determine the school of a student by their CWID
def school_by_cwid(cwid, data_df):
    """
    Determines the school of a student by their CWID.

    Args:
    cwid (str): The CWID of the student.
    data_df (DataFrame): A DataFrame with the student data.

    Returns:
    school (str): The school of the student.
    """
    subset = data_df.loc[data_df["CWID"] == cwid, "Major"]
    if not subset.empty:
        major = subset.iloc[0].lower()
    else:
        major = None
    return school_by_major(major, school_majors)


# Determine the school of the nominees a student voted for
def get_nominees_school(row, candidate_column_name):
    """
    Determines the school of the nominees a student voted for.

    Args:
    row (Series): A row from the votes DataFrame.
    candidate_column_name (str): The name of the column with the nominees.

    Returns:
    school (str): The school of the nominees.
    candidates (list): A list of the candidates.
    """
    schools = {
        candidate_column_name: "ses",
        candidate_column_name + ".1": "sob",
        candidate_column_name + ".2": "sse",
        candidate_column_name + ".3": "hass",
    }
    for column, school in schools.items():
        candidates = row[column]
        if isinstance(candidates, str):
            return school, candidates
    return None, None


# Verify a vote and add it to the votes dictionary
def verify_vote(
    row, votes, voting_record, voter_cwids, data_df, school, candidate_column_name
):
    """
    Verifies a vote and adds it to the votes dictionary.

    Args:
    row (Series): A row from the votes DataFrame.
    votes (dict): A dictionary where each key-value pair is a candidate and the number of votes they received.
    voting_record (dict): A dictionary where each key-value pair is a type of vote and the number of votes of that type.
    voter_cwids (set): A set of CWIDs of the voters.
    data_df (DataFrame): A DataFrame with the student data.
    school (str): The school of the election.
    candidate_column_name (str): The name of the column with the nominees.

    Returns:
    None
    """
    voter_school = school_by_cwid(row["Campus Wide ID (CWID)"], data_df)
    nominee_school, candidate_list = get_nominees_school(row, candidate_column_name)

    if candidate_list is None or nominee_school is None:
        logging.warning(f"Invalid vote: {row}\n{'-'*100}")
        voting_record["invalid"] += 1
        return
    elif nominee_school != school or voter_school != school:
        voting_record["wrong_school"] += 1
        return
    elif str(row["Campus Wide ID (CWID)"]) in voter_cwids:
        logging.warning(f"Duplicate vote: {row}\n{'-'*100}")
        voting_record["duplicate"] += 1
        return
    else:
        voter_cwids.add(str(row["Campus Wide ID (CWID)"]))
        candidate_list = candidate_list.split(", ")
        add_votes(candidate_list, votes, voting_record)


# Iterate through the votes DataFrame and verify each vote
def iterate_votes(
    votes, voting_record, voter_cwids, data_df, votes_df, school, candidate_column_name
):
    """
    Iterates through the votes DataFrame and verifies each vote.

    Args:
    votes (dict): A dictionary where each key-value pair is a candidate and the number of votes they received.
    voting_record (dict): A dictionary where each key-value pair is a type of vote and the number of votes of that type.
    voter_cwids (set): A set of CWIDs of the voters.
    data_df (DataFrame): A DataFrame with the student data.
    votes_df (DataFrame): A DataFrame with the voting responses.
    school (str): The school of the election.
    candidate_column_name (str): The name of the column with the nominees.

    Returns:
    None
    """
    for index, row in tqdm(votes_df.iterrows(), total=votes_df.shape[0]):
        verify_vote(
            row,
            votes,
            voting_record,
            voter_cwids,
            data_df,
            school,
            candidate_column_name,
        )


# Sort the votes dictionary by the number of votes
def sort_votes(votes):
    """
    Sorts the votes dictionary by the number of votes.

    Args:
    votes (dict): A dictionary where each key-value pair is a candidate and the number of votes they received.

    Returns:
    votes (dict): A dictionary where each key-value pair is a candidate and the number of votes they received sorted by the number of votes.
    """
    return dict(sorted(votes.items(), key=lambda item: item[1], reverse=True))


# Group the votes dictionary by the number of votes
def group_votes_by_num(votes):
    """
    Groups the votes dictionary by the number of votes.

    Args:
    votes (dict): A dictionary where each key-value pair is a candidate and the number of votes they received.

    Returns:
    votes_by_num (dict): A dictionary where each key-value pair is the number of votes and a list of the candidates that received that number of votes.
    """
    votes_by_num = defaultdict(list)
    for key, value in votes.items():
        votes_by_num[value].append(key)
    return votes_by_num


# Determine the elected candidates and if there are any tied candidates
def determine_elected(votes_by_num, num_seats):
    """
    Determines the elected candidates and if there are any tied candidates.

    Args:
    votes_by_num (dict): A dictionary where each key-value pair is the number of votes and a list of the candidates that received that number of votes.
    num_seats (int): The number of seats in the election.

    Returns:
    elected (list): A list of the elected candidates.
    remaining (int): The number of remaining seats.
    tied_elected (list): A list of the tied candidates.
    """
    elected, remaining, tied_elected = [], None, None
    for key, value in sorted(votes_by_num.items(), reverse=True):
        if len(value) <= num_seats - len(elected):
            elected.extend(value)
        else:
            remaining = num_seats - len(elected)
            tied_elected = value
            break
    return elected, remaining, tied_elected


# Display the results of the election
def print_output(
    num_seats, votes, voting_record, voter_cwids, elected, remaining, tied_elected
):
    """
    Displays the results of the election.

    Args:
    num_seats (int): The number of seats in the election.
    votes (dict): A dictionary where each key-value pair is a candidate and the number of votes they received.
    voting_record (dict): A dictionary where each key-value pair is a type of vote and the number of votes of that type.
    voter_cwids (set): A set of CWIDs of the voters.
    elected (list): A list of the elected candidates.
    remaining (int): The number of remaining seats.
    tied_elected (list): A list of the tied candidates.

    Returns:
    None
    """
    print(f"\nThe voting record is:")
    for key, value in voting_record.items():
        print(f"    {key}: {value}")
    print(
        f"\nThe {len(elected)} elected candidate{'s are' if len(elected) > 1 else ' is'}:"
    )
    for candidate in elected:
        print(f"    {candidate}")
    if tied_elected is not None and remaining > 0:
        print(
            f"\nThere {'are' if remaining > 1 else 'is'} {remaining} seat{'s' if remaining > 1 else ''} remaining. The tied candidates are: {tied_elected}"
        )
    elif len(elected) == len(votes.keys()):
        remaining_seats = num_seats - len(elected)
        print(
            f"\nEveryone was elected. There {'are' if remaining_seats > 1 else 'is'} {remaining_seats} seat{'s' if remaining_seats > 1 else ''} remaining."
        )


# Function to run the program
def main():
    """
    Runs the program and determines the election results.

    Returns:
    None
    """
    config = load_config()

    voting_file = find_csv_file()
    print(f"\nIdentified the voting csv file as '{voting_file}'")
    data_df, votes_df = load_data(config["data_file"], voting_file)

    setup_logger()

    votes, voting_record, voter_cwids = initialize_setups()

    iterate_votes(
        votes,
        voting_record,
        voter_cwids,
        data_df,
        votes_df,
        config["school"],
        config["candidate_column_name"],
    )
    votes = sort_votes(votes)
    votes_by_num = group_votes_by_num(votes)
    elected, remaining, tied_elected = determine_elected(
        votes_by_num, config["num_seats"]
    )

    print_output(
        config["num_seats"],
        votes,
        voting_record,
        voter_cwids,
        elected,
        remaining,
        tied_elected,
    )


# Function to run the program if this file is run directly
if __name__ == "__main__":
    main()
