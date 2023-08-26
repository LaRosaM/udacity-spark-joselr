# STEDI Udacity Project
This project has been developed by the student JoseLR for the STEDI Human Balance Analytics project.

It's needeed to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

This project has 3 layers for data management:

The Landing layer where all the raw data from accelerometers, consumers and step trainers will be loaded.

The Trusted layer where we will load the information of consumers who have given their permission to be part of the research.

The Curated layer where we will clean and aggregate all this information and relate it to each other so that it is ready to be exploited by the data scientists.

This project has some screenshots that allow us to visualize the work done.