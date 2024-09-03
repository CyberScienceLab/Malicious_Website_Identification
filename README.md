# Advancing Malicious Website Identification: A Machine Learning Approach Using Granular Feature Analysis

This repository is part of a collaborative research initiative between the University of Guelph, MITACS, and Arctic Wolf Networks. The project focuses on designing, implementing, and training a machine learning model to classify websites into detailed malicious categories with high accuracy. This repository contains all resources used throughout the project for data collection, aggregation, model training, and analytical figure generation.

## Repository Structure

- **`data_construction`**: Scripts for data collection and aggregation. This directory processes raw data into a structured format for analysis.
- **`data`**: Aggregated data in a compressed format, ready for use in model training.
- **`experiments`**: Code used for the machine learning model's training. This includes implementation of algorithms and parameter tuning.
- **`figures`**: Visual representations of the analysis and results from the model training.

## Dataset Overview

The dataset contains 441,707 samples, broken down as follows:
- **Benign**: 235,721
- **Phishing**: 73,345
- **Command and Control**: 66,490
- **Spam**: 46,009
- **Malware Hosting**: 16,726
- **Malicious Advertisement Hosting**: 3,085
- **Host Scanners**: 231
- **Exploit Kits**: 82
- **Credit Card Skimmers**: 12
- **Source Exploits**: 4
- **Web Attackers**: 2

## Data Collection Sources

Data was sourced from various threat intelligence sharing platforms, including IBM, abuse.ch, and LevelBlue Labs, providing extensive datasets for training and validating the machine learning model.

## Contributing

We welcome contributions from researchers and practitioners who are interested in improving the model or expanding the dataset. Contributions can be made via pull requests or issues in this GitHub repository.


