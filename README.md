# README: Project QDSeeds

## Project Overview
**QDSeeds** is a data acquisition and processing initiative designed to seed **QDArchive**, a web service for researchers to archive and publish qualitative data. This project addresses the "chicken and egg" problem of attracting researchers to a new platform by pre-populating it with existing, openly licensed qualitative research data.

The project focuses on capturing **Qualitative Data Analysis (QDA) files** (e.g., .qdpx, .mx24), which provide structured insights into primary data like interview transcripts.

---

## Student Information
* **Name:** Paromita Das
* **Email:** paromita.das@fau.de
* **Student ID:** 23460503
* **Institution:** FAU Erlangen
* **Course:** Winter 2025 / 26 + Summer 2026

---

## Part 1: Data Acquisition
The acquisition phase involved building a pipeline to programmatically extract research project folders, metadata, and associated files.

### Data Sources & Extraction
I developed a **Python CLI application** that utilizes repository APIs to query and download research data.

| Source | Reference ID | Query / Method | Projects Extracted |
| :--- | :--- | :--- | :--- |
| **CESSDA** | 13 | API Query: "interview questions" | 2,943 |
| **QDR** | 4 | API Query | 1,477 |

### Implementation Details
* **Pipeline:** The application downloads the entire research project folder, including metadata.
* **Storage:** All metadata and file paths are stored in a **SQLite database** .
* **File Types:** The system targets **Analysis Data** (QDA files), **Primary Data** (transcripts/PDFs), and **Additional Data**.
* **Licensing:** Only data with **open licenses** (e.g., Creative Commons) are stored; projects without licenses are excluded as proprietary.

---

## Technical Challenges & Deliverables
* **Technical Challenges:** Ongoing reporting of challenges related to data formats and repository structures as well as the difficulty of downloading the attachments that some are not available.
* **Output:** A database export and a folder containing all downloaded research files.

---
