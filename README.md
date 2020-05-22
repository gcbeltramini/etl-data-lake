# ETL - Data Lake

A music streaming startup, Sparkify, has grown their user base and song database even more and want
to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs
on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal is to build an ETL pipeline to:

1. extract their data from S3;
1. process the data using Spark;
1. load the data back into S3 as a set of dimensional tables.

This will allow their analytics team to continue finding insights in what songs their users are
listening to.

## Contents

1. [Project datasets](docs/project-datasets.md)
1. [Schema for song play analysis](docs/schema.md)
1. [Project structure](docs/project-structure.md)
1. [Run the ETL](docs/running.md)
