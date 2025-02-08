#!/bin/bash
set -e  # Stop the script if any command fails
set -u  # Stop if undefined variable is used
# Function to run R script and check for errors
run_r_script() {
    if ! Rscript "$@"; then
        echo "Error running Rscript with arguments: $@"
        exit 1
    fi
}

# clean data and estimation
run_r_script -e "source('main/01_clean_data.R')"
run_r_script -e "source('main/02_1_transform_data.R')"
run_r_script -e "source('main/02_2_construct_matching_data.R')"
# julia "main/04_1_estimate_maximum_score.jl"
# julia "main/04_2_counterfactual_maximum_score.jl"

# report
run_r_script -e "rmarkdown::render('main/01_clean_data.Rmd')"
run_r_script -e "rmarkdown::render('main/03_construct_figuretable_merger.Rmd')"
run_r_script -e "rmarkdown::render('main/04_3_construct_figuretable_maximum_score.Rmd')"