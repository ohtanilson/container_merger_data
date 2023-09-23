using CSV
using Plots
using JuMP#, Ipopt
using Random
using Distributions
using LinearAlgebra
using Dates
using DataFrames
using Gurobi
using DataFramesMeta
using Combinatorics
using Optim
using BlackBoxOptim # on behalf of DEoptim
# using LaTeXTbuyer_operator_age_normalized ulars
# using LaTeXStrings
# using DelimitedFiles
using VegaLite
using Distributed
Distributed.@everywhere include("../main/04_0_functions.jl")
matching_pair_year_CIY = VegaLite.load("../container_merger_data/output/matching_pair_year_CIY.rds")
matching_pair_year_IHS = VegaLite.load("../container_merger_data/output/matching_pair_year_IHS.rds")
matching_pair_year_HB = VegaLite.load("../container_merger_data/output/matching_pair_year_HBdata.rds")
data_CIY = 
    DataFramesMeta.@chain matching_pair_year_CIY begin
    DataFramesMeta.@select :seller_operator_age_normalized :seller_cumsum_TEU_normalized :buyer_operator_age_normalized :buyer_cumsum_TEU_normalized :seller_lat :seller_lon :buyer_lat :buyer_lon
    end
data_IHS = 
    DataFramesMeta.@chain matching_pair_year_IHS begin
    DataFramesMeta.@select :seller_operator_age_normalized :seller_cumsum_TEU_normalized :buyer_operator_age_normalized :buyer_cumsum_TEU_normalized :seller_lat :seller_lon :buyer_lat :buyer_lon
    end    
data_HB = 
    DataFramesMeta.@chain matching_pair_year_HB begin
    DataFramesMeta.@select :seller_operator_age_normalized :seller_cumsum_TEU_normalized :buyer_operator_age_normalized :buyer_cumsum_TEU_normalized :seller_lat :seller_lon :buyer_lat :buyer_lon
    end

#---------------#
# Estimation
#---------------#
# CIY_data
Random.seed!(1)
@time param_list_CIY, correct_num_match_list_CIY, all_ineq_num_CIY = 
    estimate_maximum_score(
    data_CIY,
    n_estimation = 100
    )
#5.286121 seconds (18.73 M allocations: 1.556 GiB, 7.35% gc time)
maximum(correct_num_match_list_CIY) - minimum(correct_num_match_list_CIY)
correct_match_percent = 
    correct_num_match_list_CIY/all_ineq_num_CIY
filename = "../container_merger_data/output/param_list_CIY.csv"
temp_results_for_output = DataFrame([param_list_CIY correct_match_percent], :auto)
CSV.write(filename, temp_results_for_output)

# IHS_data
@time param_list_IHS, correct_num_match_list_IHS, all_ineq_num_IHS = 
    estimate_maximum_score(
    data_IHS,
    n_estimation = 100
    )
#5.286121 seconds (18.73 M allocations: 1.556 GiB, 7.35% gc time)
maximum(correct_num_match_list_IHS) - minimum(correct_num_match_list_IHS)
correct_match_percent = 
    correct_num_match_list_IHS/all_ineq_num_IHS
filename = "../container_merger_data/output/param_list_IHS.csv"
temp_results_for_output = DataFrame([param_list_IHS correct_match_percent], :auto)
CSV.write(filename, temp_results_for_output)

# HB_data
@time param_list_HB, correct_num_match_list_HB, all_ineq_num_HB = 
    estimate_maximum_score(
    data_HB,
    n_estimation = 100
    )
maximum(correct_num_match_list_HB) - minimum(correct_num_match_list_HB)
correct_match_percent = 
    correct_num_match_list_HB/all_ineq_num_HB
#116.599478 seconds (744.50 M allocations: 31.232 GiB, 3.93% gc time)
filename = "../container_merger_data/output/param_list_HB.csv"
temp_results_for_output = DataFrame([param_list_HB correct_match_percent], :auto)
CSV.write(filename, temp_results_for_output)


