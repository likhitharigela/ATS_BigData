import os
import json
import PyPDF2
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import re

# Initialize spaCy model for NER (using a smaller model for faster processing)
nlp = spacy.load("en_core_web_md")  # Changed to use the "en_core_web_md" model

# Initialize Spark session
spark = SparkSession.builder.appName("ResumeExtraction").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("dob", StringType(), True),  # Added dob
    StructField("cgpa", StringType(), True),  # Added cgpa
    StructField("skills", StringType(), True),  # Added skills
    StructField("error", StringType(), True)
])

# Function to extract text from PDF
def extract_pdf_text(pdf_path):
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            text = ""
            for page in range(len(reader.pages)):
                text += reader.pages[page].extract_text()
        return text
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return None

# Function to extract skills from resume text using spaCy and predefined keywords
def extract_skills(text):
    doc = nlp(text)
    skills = []

    # Predefined list of skills (you can expand this as needed)
    predefined_skills = ['Python', 'Java', 'C++', 'TensorFlow', 'Machine Learning', 'Data Science', 'SQL', 'JavaScript', 'R', 'Pandas', 'Scikit-learn']

    # Check for predefined skills in text
    for skill in predefined_skills:
        if skill.lower() in text.lower():
            skills.append(skill)

    # Use NER to detect additional skills (e.g., companies or tools)
    for ent in doc.ents:
        if ent.label_ == 'ORG':  # Organizations may also indicate tools or skills
            skills.append(ent.text)

    # Remove duplicates and return the skills as a comma-separated string
    return ', '.join(set(skills))

# Function to extract name, email, phone number, dob, and cgpa from resume text
def extract_resume_data(text):
    name, email, phone, dob, cgpa = None, None, None, None, None

    # Extract name with stricter rules: avoid common section titles like "Address", "Curriculum Vitae", etc.
    name_pattern = r"^[A-Z][a-zA-Z\s]+(?:\s+[A-Za-z]+)*"  # Match a more specific name pattern, avoiding keywords
    name_match = re.match(name_pattern, text)
    if name_match:
        name = name_match.group(0)

    # Extract email
    email_match = re.search(r"[\w\.-]+@[\w\.-]+", text)
    if email_match:
        email = email_match.group(0)

    # Extract phone number (improved regex for better accuracy)
    phone_pattern = r"(?:\+91[\s\-]?)?([789]\d{9})"  # Regex for Indian phone number
    phone_match = re.search(phone_pattern, text)
    if phone_match:
        phone = phone_match.group(1).strip()  # Only capture the 10-digit phone number

    # Extract Date of Birth (DOB or Date of Birth patterns)
    dob_pattern = r"(?:dob|date of birth|DOB|Date of Birth)[\s:]*([0-9]{1,2}[/\-\s]?[0-9]{1,2}[/\-\s]?[0-9]{4})"
    dob_match = re.search(dob_pattern, text, re.IGNORECASE)
    if dob_match:
        dob = dob_match.group(1).strip()

    # Extract CGPA (matching values out of 10, e.g., 9.0, 8.5, 7.3)
    cgpa_pattern = r"\b([0-9]{1,2}(\.\d{1,2})?)\s*(\/|out of)\s*10\b"
    cgpa_match = re.search(cgpa_pattern, text)
    if cgpa_match:
        cgpa = cgpa_match.group(1).strip()

    return name, email, phone, dob, cgpa

# Process resumes in the input folder (e.g., Spark_Module_A)
def process_resumes(input_folder, output_folder):
    result = []
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.json'):
            file_path = os.path.join(input_folder, file_name)
            
            # Read the input JSON metadata file
            with open(file_path, 'r') as f:
                for line in f:
                    try:
                        metadata = json.loads(line)  # Parse each line as a separate JSON object
                        
                        # Debugging: print the parsed metadata to inspect the structure
                        print(f"Parsed metadata: {metadata}")

                        # Handle metadata being either a dictionary or a list of dictionaries
                        if isinstance(metadata, dict):
                            # Single dictionary per line case
                            metadata_entries = [metadata]
                        elif isinstance(metadata, list):
                            # Handle case where metadata is a list of dictionaries
                            metadata_entries = metadata
                        else:
                            print(f"Unexpected metadata format: {metadata}")
                            continue

                        # Process each entry in metadata_entries
                        for entry in metadata_entries:
                            resume_path = entry.get('file_path')
                            if resume_path and os.path.exists(resume_path):
                                # Extract text from the PDF resume
                                resume_text = extract_pdf_text(resume_path)
                                if resume_text:
                                    # Extract name, email, phone, dob, cgpa, and skills using the updated logic
                                    name, email, phone, dob, cgpa = extract_resume_data(resume_text)
                                    skills = extract_skills(resume_text)  # Extract skills using spaCy and predefined keywords
                                    result.append({
                                        "file_name": entry.get("file_name", "Unknown"),
                                        "name": name,
                                        "email": email,
                                        "phone": phone,
                                        "dob": dob,
                                        "cgpa": cgpa,
                                        "skills": skills,  # Add extracted skills
                                        "error": None
                                    })
                                else:
                                    result.append({
                                        "file_name": entry.get("file_name", "Unknown"),
                                        "name": None,
                                        "email": None,
                                        "phone": None,
                                        "dob": None,
                                        "cgpa": None,
                                        "skills": None,
                                        "error": "Failed to extract text"
                                    })
                            else:
                                result.append({
                                    "file_name": entry.get("file_name", "Unknown"),
                                    "name": None,
                                    "email": None,
                                    "phone": None,
                                    "dob": None,
                                    "cgpa": None,
                                    "skills": None,
                                    "error": "File not found"
                                })
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON in file {file_name}: {e}")
                        continue

    # Create a Spark DataFrame with the defined schema and save the result as JSON
    result_rdd = spark.sparkContext.parallelize(result)
    result_df = spark.createDataFrame(result_rdd, schema)
    
    # Save the processed data to the output directory
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    output_path = os.path.join(output_folder, "processed_resumes_A")
    result_df.write.json(output_path, mode="overwrite")
    print(f"Processed resume data saved to {output_path}")

# Run the extraction process for Spark_Module_A
process_resumes("distribution/Spark_Module_A", "processed_resumes_A")

