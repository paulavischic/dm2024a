#!/bin/bash 
source ~/.venv/bin/activate  
kaggle competitions submissions -c itba-data-mining-2024-a -v > competicion.csv
deactivate 
