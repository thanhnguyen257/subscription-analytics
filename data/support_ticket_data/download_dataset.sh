#!/bin/bash
curl -L -o ./kaggle_1.zip\
  https://www.kaggle.com/api/v1/datasets/download/ajverse/customer-support-tickets-crm-dataset

unzip -o kaggle_1.zip
mv customer_support_tickets.csv customer_support_tickets_1.csv

curl -L -o ./kaggle_2.zip\
  https://www.kaggle.com/api/v1/datasets/download/suraj520/customer-support-ticket-dataset

unzip -o kaggle_2.zip
mv customer_support_tickets.csv customer_support_tickets_2.csv

find . -maxdepth 1 -type f \
  ! -name "download_dataset.sh" \
  ! -name "customer_support_tickets_1.csv" \
  ! -name "customer_support_tickets_2.csv" \
  -delete