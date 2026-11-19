# Project Milestone 2

## Wymagania zaliczenia (Completion Requirements)
* **Otwarto:** piątek, 2 października 2026, 00:00
* **Wymagane do:** poniedziałek, 23 listopada 2026, 23:59

### Overview & Attendance
* The primary objective of this milestone is to develop data acquisition and pre-processing modules and to present the results during a scheduled project meeting.
* All team members are required to attend the project presentation session (90-minute slot).
* Active participation is strongly encouraged, including asking questions to other teams, sharing feedback, and providing recommendations.

---

## Deliverables
*(To be submitted by the Project Manager)*

* [ ] **Project Report** in PDF format, named `TeamName_Report.pdf`.
* [ ] **Presentation Document** in PDF format, named `TeamName_Presentation.pdf`.
* [ ] **Test Documentation** in PDF format, named `TeamName_Tests.pdf`.
* [ ] **Link to the Git Repository** containing project source code, configuration files, and supplementary documentation.

---

## Detailed Requirements for the Project Report

### 1. Updates to PM1 Report
* Revisions to business and technical assumptions, with clear justifications for any modifications.

### 2. Conceptual Data Model
* Identification of key entities and their relationships.

### 3. Description of Data Sets (from all data sources)
* Data category (e.g., location of public transport buses).
* Average number of records and frequency of data inflow.
* Features describing each record.
* Time period covered by the data.
* Data quality assessment.
* Source data format (e.g., JSON, CSV, XML).
* **For datasets or streams intended as input for classification or regression tasks:**
  * List and definitions of independent and dependent features.
  * Description of transformations applied to raw data for machine learning purposes.

### 4. Data Acquisition and Transformation
* Acquisition strategy (technologies, libraries, API limitations).
* Data transformation steps and storage strategy (technologies, formats).

### 5. System Architecture
* Diagram and detailed description of the solution architecture.
* Inclusion of all technologies and tools (implemented and planned).
* Description of data flows.
* Clear alignment with either **Lambda** or **Kappa** architecture, explicitly marked on the diagram.
* *For Kappa architecture:* demonstration of reprocessing for historical data streams.

### 6. Analytical Approach
* Planned types of machine learning tasks (classification, regression, clustering, etc.) and/or other analytical tasks.
* Intended use of batch and stream processing for each task.
* Proposed methods for presenting results to end users in subsequent stages of the project.

---

## Detailed Requirements for Testing

* **Functional Tests** for data acquisition and pre-processing modules.
* **Quality Tests** for data transformation processes.
* **Tabular Test Case Documentation**, including:
  * Test objective
  * Test steps
  * Expected result
  * Actual result (e.g., screenshot, log, summary report)

---

## Review & Final Submission Process

> **Note:** The project team is required to reserve a time slot in the **ProjectMeetings** register (available on MS Teams) in order to discuss the report with the project supervisor.
> 
> Following the discussion, the project team may revise the report and submit the final version by **Wednesday EOD**.