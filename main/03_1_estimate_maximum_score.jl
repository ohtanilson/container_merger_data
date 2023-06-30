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
#include("03_0_functions.jl")
matching_pair_year_CIY = VegaLite.load("../container_merger_data/output/matching_pair_year_CIY.rds")
matching_pair_year_HB = VegaLite.load("../container_merger_data/output/matching_pair_year_HB.rds")
data_CIY = 
    DataFramesMeta.@chain matching_pair_year_CIY begin
    DataFramesMeta.@select :seller_operator_age_normalized :seller_cumsum_TEU_normalized :buyer_operator_age_normalized :buyer_cumsum_TEU_normalized
    end
data_HB = 
    DataFramesMeta.@chain matching_pair_year_HB begin
    DataFramesMeta.@select :seller_operator_age_normalized :seller_cumsum_TEU_normalized :buyer_operator_age_normalized :buyer_cumsum_TEU_normalized
    end

#---------------#
# Estimation
#---------------#
function score_b(
    beta::Vector{Float64},
    data::DataFrame
    )
    beta = beta[1] # for Optim
    A = kron(data.buyer_operator_age_normalized , data.seller_operator_age_normalized') #Take care of row and column
    B = kron(data.buyer_cumsum_TEU_normalized, data.seller_cumsum_TEU_normalized') #Take care of row and column
    temp = [Combinatorics.combinations(1:size(data)[1],2)...]
    index_list = Array{Int64,2}(undef, length(temp), 2)
    for i = 1:length(temp)
        index_list[i,1] = temp[i][1]
        index_list[i,2] = temp[i][2]
    end
    ineqs = fill(-1000.0, length(index_list[:,1]))
    comper = 1*A + beta*B
    for j in 1:length(index_list[:,1])
        ineqs[j] = ineq(comper, index_list[j,:])
    end
    res = sum(ineqs.>0)
    return res
end

function scorethis(
    beta::Vector{Float64},
    obsdat::DataFrame
    )
    res = -1.0*score_b(beta, obsdat) + 100000.0 # need to be Float64 for buyer_cumsum_TEU_normalizedoptimize
    return res
end

function estimate_maximum_score(
    data;
    n_estimation = 100
    )
    param_list = zeros(n_estimation)
    correct_num_match_list = zeros(n_estimation)
    temp = [Combinatorics.combinations(1:size(data)[1],2)...]
    all_ineq_num = length(temp)
    for i = 1:n_estimation
        m_res = 
        BlackBoxOptim.bboptimize(
            beta -> scorethis(beta, data);
            SearchRange = (-10.0, 10.0),
            NumDimensions = length(1),
            Method = :de_rand_1_bin,
            MaxSteps = 1000
            )
        param_list[i] = m_res.archive_output.best_candidate[1]
        correct_num_match_list[i] = 100000 - m_res.archive_output.best_fitness
    end
    return param_list, correct_num_match_list, all_ineq_num
end



# CIY_data
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
# HB_data
@time param_list_HB, correct_num_match_list_HB, all_ineq_num_HB = 
    estimate_maximum_score(
    data_HB,
    n_estimation = 100
    )
maximum(correct_num_match_list_HB) - minimum(correct_num_match_list_HB)
correct_match_percent = 
    correct_num_match_list_HB/all_ineq_num_HB
#38.917446 seconds (197.44 M allocations: 17.779 GiB, 8.13% gc time)
filename = "../container_merger_data/output/param_list_HB.csv"
temp_results_for_output = DataFrame([param_list_HB correct_match_percent], :auto)
CSV.write(filename, temp_results_for_output)
