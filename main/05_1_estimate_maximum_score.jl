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
function expand_grid(args...)
    nargs= length(args)
    if nargs == 0
      error("expand_grid need at least one argument")
    end
    iArgs= 1:nargs
    nmc= "Var" .* string.(iArgs)
    nm= nmc
    d= map(length, args)
    orep= prod(d)
    rep_fac= [1]
    # cargs = []
    if orep == 0
        error("One or more argument(s) have a length of 0")
    end
    cargs= Array{Any}(undef,orep,nargs)
    for i in iArgs
        x= args[i]
        nx= length(x)
        orep= Int(orep/nx)
        mapped_nx= vcat(map((x,y) -> repeat([x],y), collect(1:nx), repeat(rep_fac,nx))...)
        cargs[:,i] .= x[repeat(mapped_nx,orep)]
        rep_fac= rep_fac * nx
    end
    #convert(DataFrame,cargs)
    DataFrame(cargs, :auto)
end


function ineq(
    mat::Array{Float64,2},
    idx::Vector{Int64}
    )
    prin = mat[idx,idx]
    ineq = prin[1,1]+prin[2,2]-prin[1,2]-prin[2,1]
    return ineq
end
function compute_distance_from_lat_long(
    seller_lat,
    buyer_lat,
    seller_lon,
    buyer_lon
    )
    distance_1000km =
        acos(
        sin(seller_lat)*sin(buyer_lat) + 
          cos(seller_lat) * cos(buyer_lat) *
          cos(seller_lon - buyer_lon)
        ) * 6378.137/ 1000 
    # standardized to [0,1]
    distance_1000km = distance_1000km/20
    return distance_1000km
end

function score_b(
    beta::Vector{Float64},
    data::DataFrame
    )
    beta1 = beta[1] # for Optim
    beta2 = beta[2] # for Optim
    distance_1000km = zeros(size(data)[1], size(data)[1])
    for ii = 1:size(data)[1]
        for jj = 1:size(data)[1]
            seller_lat = data.seller_lat[ii]
            seller_lon = data.seller_lon[ii]
            buyer_lat = data.buyer_lat[jj]
            buyer_lon = data.buyer_lon[jj]
            distance_1000km[ii,jj] =
                compute_distance_from_lat_long(
                    seller_lat,
                    buyer_lat,
                    seller_lon,
                    buyer_lon
                    )
        end
    end
    A = kron(data.buyer_operator_age_normalized, data.seller_operator_age_normalized') #Take care of row and column
    B = kron(data.buyer_cumsum_TEU_normalized, data.seller_cumsum_TEU_normalized') #Take care of row and column
    C = distance_1000km 
    temp = [Combinatorics.combinations(1:size(data)[1],2)...]
    index_list = Array{Int64,2}(undef, length(temp), 2)
    for i = 1:length(temp)
        index_list[i,1] = temp[i][1]
        index_list[i,2] = temp[i][2]
    end
    ineqs = fill(-1000.0, length(index_list[:,1]))
    comper = 1*A + beta1*B + beta2*C
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
    res = -1.0*score_b(beta, obsdat) +
          100000.0 # need to be Float64 for buyer_cumsum_TEU_normalizedoptimize
    return res
end

function estimate_maximum_score(
    data;
    n_estimation = 100
    )
    dim_parameter = 2
    param_list = zeros(n_estimation, dim_parameter)
    correct_num_match_list = zeros(n_estimation)
    temp = [Combinatorics.combinations(1:size(data)[1],2)...]
    all_ineq_num = length(temp)
    for i = 1:n_estimation
        m_res = 
        BlackBoxOptim.bboptimize(
            beta -> scorethis(beta, data);
            SearchRange = (-10.0, 10.0),
            NumDimensions = dim_parameter,
            Method = :de_rand_1_bin,
            MaxSteps = 1000
            )
        param_list[i,:] = m_res.archive_output.best_candidate
        correct_num_match_list[i] = 
            100000 - 
            m_res.archive_output.best_fitness
    end
    return param_list, correct_num_match_list, all_ineq_num
end


function matchval2(Ab,At,Bb,Bt,seller_lat,seller_lon,buyer_lat,buyer_lon,true_β)
    distance_term = zeros(length(Ab))
    distance_1000km = zeros(length(Ab))
    for ii = 1:length(Ab)
        distance_1000km[ii] =
            compute_distance_from_lat_long(
                seller_lat[ii],
                seller_lon[ii],
                buyer_lat[ii],
                buyer_lon[ii]
                )
        if distance_1000km[ii] == 0
            distance_term[ii] = -9999
        else
            distance_term[ii] = true_β[2].*distance_1000km[ii]
        end
    end
	val = 1.0.*Ab.*At .+
          true_β[1].*Bb.*Bt .+
          distance_term
  return val
end
function givemedata2(
    ;
    data = data_IHS,
    sd_err = 1.0,
    true_β = [1, 1],
    random_seed = 1
    )
    Random.seed!(random_seed)
    # buydata = rand(Distributions.MvNormal(means, covars), N)
    buydata = 
        DataFramesMeta.@chain data begin
        DataFramesMeta.@select :buyer_operator_age_normalized :buyer_cumsum_TEU_normalized :buyer_lat :buyer_lon
    end  
    N = size(buydata)[1]
    buyid = Array{Int64,1}(1:N)
    buydata = hcat(buyid, buydata)
    #buydata = convert(DataFrame, buydata)
    rename!(buydata, [:id, :Ab, :Bb, :buyer_lat, :buyer_lon])

    #tardata = rand(Distributions.MvNormal(means, covars), N)
    tardata = 
        DataFramesMeta.@chain data begin
        DataFramesMeta.@select :seller_operator_age_normalized :seller_cumsum_TEU_normalized :seller_lat :seller_lon
    end  

    tarid = Array((1+N):(N+N))
    # non-interactive term
    println("non-interactive Match specific term: Ct = rnorm(N, 10, 1)")
    #Ct = rand(Distributions.Normal(10, 1), N)
    tardata = hcat(tarid, tardata)
    #tardata = convert(DataFrame, tardata)
    rename!(tardata, [:id, :At, :Bt, :seller_lat, :seller_lon])

    matchmaker = expand_grid(buyid, tarid)
    rename!(matchmaker, [:buyid, :tarid])
    matchdat = DataFrames.leftjoin(matchmaker, tardata, on = [:tarid => :id])
    matchdat = DataFrames.leftjoin(matchdat, buydata, on = [:buyid => :id])
    sort!(matchdat, [:buyid, :tarid]);
    #matchdat = within(matchdat, mval <- matchval(Ab,At,Bb,Bt))
    mval = matchval2(
           matchdat.Ab,
           matchdat.At,
           matchdat.Bb,
           matchdat.Bt,
           matchdat.buyer_lat,
           matchdat.buyer_lon,
           matchdat.seller_lat,
           matchdat.seller_lon,
           true_β
           )
    #matchdat = within(matchdat, mval <- mval + rnorm(length(matchdat$mval), mean = 0, sd_err) )
    mval = mval .+ rand(Distributions.Normal(0, sd_err), length(mval))
    matchdat = hcat(matchdat, mval)
    rename!(matchdat, :x1 => :mval)

    obj = matchdat.mval
    rhs = ones(N + N)
    utility = zeros(N,N)
    for i = 1:N
        for j = 1:N
            utility[i,j] = obj[(i-1)*N+j]
        end
    end

    model = JuMP.Model(Gurobi.Optimizer)
    set_optimizer_attribute(model, "TimeLimit", 100)
    set_optimizer_attribute(model, "Presolve", 0)
    JuMP.@variable(model, 0<=x[i=1:N,j=1:N]<=1)
    @constraint(model, feas_i[i=1:N], sum(x[i,j] for j in 1:N)<= 1)
    @constraint(model, feas_j[j=1:N], sum(x[i,j] for i in 1:N)<= 1)
    JuMP.@objective(model, Max, sum(x[i,j]*utility[i,j] for i in 1:N, j in 1:N))
    println("Time for optimizing model:")
    @time JuMP.optimize!(model)
    # show results
    objv = JuMP.objective_value(model)
    println("objvalue　= ", objv)
    matches = JuMP.value.(x)
    # restore unmatched
    unmatched_buyid = [1:1:N;][vec(sum(matches,dims=2) .== 0)]
    unmatched_tarid = [(N+1):1:(N+N);]'[sum(matches,dims=1) .== 0]
    matches = vec(matches')
    matchdat = hcat(matchdat, matches)
    rename!(matchdat, :x1 => :matches)
    model = JuMP.Model(Gurobi.Optimizer)
    set_optimizer_attribute(model, "TimeLimit", 100)
    set_optimizer_attribute(model, "Presolve", 0)
    JuMP.@variable(model, 0 <= u[i=1:N])
    JuMP.@variable(model, 0 <= v[i=1:N])
    @constraint(model,
                dual_const[i=1:N,j=1:N],
                u[i]+v[j]>= utility[i,j])
    JuMP.@objective(model, Min,
                    sum(u[i] for i in 1:N) +
                     sum(v[j] for j in 1:N))
    println("Time for optimizing model:")
    @time JuMP.optimize!(model)
    duals = vcat(JuMP.value.(u), JuMP.value.(v))
    println("sum of duals equal to obj value?: ",
             round(sum(duals),digits = 4)==round(objv,digits = 4))
    # tar price must be positive!
    lo = N + 1
    hi = N + N
    duals = DataFrame(tarid=Array((1+N):(N+N)),
                      tarprice = duals[lo:hi])
    matchdat = DataFrames.leftjoin(matchdat,
                                   duals,
                                   on = [:tarid => :tarid])
    @linq obsd = matchdat |>
  	  where(:matches .== 1.0)
    # for i in unmatched_buyid
    #     @linq obsd_unmatched = matchdat |>
    #       where(:buyid .== i)
	#     	obsd_unmatched = obsd_unmatched[1:2,:]
    #     obsd_unmatched.tarid .= N + N + 2
    #     obsd_unmatched.At .= 0
    #     obsd_unmatched.Bt .= 0
	#     	obsd_unmatched.Ct .= 0
    #     obsd_unmatched.tarprice .== copy(0) #unmatched transfer
    #     obsd = vcat(obsd, obsd_unmatched)
    #     obsd = obsd[1:size(obsd)[1]-1,:]
    # end
    # for j in unmatched_tarid
    #     @linq obsd_unmatched = matchdat |>
    #       where(:tarid .== j)
	#     	obsd_unmatched = obsd_unmatched[1:2,:]
    #     obsd_unmatched.buyid .= N + N + 1
	#     	obsd_unmatched.Ab .= 0
    #     obsd_unmatched.Bb .= 0
	#     	obsd_unmatched.Cb .= 0
    #     obsd_unmatched.tarprice .== copy(0) #unmatched transfer
    #     obsd = vcat(obsd, obsd_unmatched)
    #     obsd = obsd[1:size(obsd)[1]-1,:]
    # end
    return(obsd)
end

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


# counterfactual
# IHS
distance_IHS =
    compute_distance_from_lat_long.(
        data_IHS.seller_lat,
        data_IHS.buyer_lat,
        data_IHS.seller_lon,
        data_IHS.buyer_lon
    )
data_IHS_different_coutry = 
    hcat(data_IHS, distance_IHS)
data_IHS_different_coutry =
    DataFramesMeta.@chain data_IHS_different_coutry begin
        DataFramesMeta.@subset (:x1 .> 1e-6)
    end
param_list_IHS =
    CSV.read("../container_merger_data/output/param_list_IHS.csv", DataFrame)
estimated_beta =
    param_list_IHS[param_list_IHS[:,3] .== maximum(param_list_IHS[:,3]),1:2]
max_beta1 = maximum(estimated_beta[:,1])
min_beta1 = minimum(estimated_beta[:,1])
max_beta2 = maximum(estimated_beta[:,2])
min_beta2 = minimum(estimated_beta[:,2])
# maxbeta
data = givemedata2(
    data = data_IHS_different_coutry,
    sd_err = 1.0,
    true_β = [max_beta1, max_beta2],
    random_seed = 1
    )
data = 
    DataFramesMeta.@chain data begin
    DataFramesMeta.@select :At :Bt :seller_lat :seller_lon :Ab :Bb :buyer_lat :buyer_lon
end  
rename!(data, [:seller_operator_age_normalized, :seller_cumsum_TEU_normalized, :seller_lat, :seller_lon, :buyer_operator_age_normalized, :buyer_cumsum_TEU_normalized, :buyer_lat, :buyer_lon])
filename = "../container_merger_data/output/counterfactual_IHS_max_beta.csv"
CSV.write(filename, data)
# minbeta
data = givemedata2(
    data = data_IHS_different_coutry,
    sd_err = 1.0,
    true_β = [min_beta1, min_beta2],
    random_seed = 1
    )
data = 
    DataFramesMeta.@chain data begin
    DataFramesMeta.@select :At :Bt :seller_lat :seller_lon :Ab :Bb :buyer_lat :buyer_lon
end  
rename!(data, [:seller_operator_age_normalized, :seller_cumsum_TEU_normalized, :seller_lat, :seller_lon, :buyer_operator_age_normalized, :buyer_cumsum_TEU_normalized, :buyer_lat, :buyer_lon])
filename = "../container_merger_data/output/counterfactual_IHS_min_beta.csv"
CSV.write(filename, data)

# HB

distance_HB =
    compute_distance_from_lat_long.(
        data_HB.seller_lat,
        data_HB.buyer_lat,
        data_HB.seller_lon,
        data_HB.buyer_lon
    )
data_HB_different_coutry = 
    hcat(data_HB, distance_HB)
data_HB_different_coutry =
    DataFramesMeta.@chain data_HB_different_coutry begin
        DataFramesMeta.@subset (:x1 .> 1e-6)
    end
param_list_HB =
    CSV.read("../container_merger_data/output/param_list_HB.csv", DataFrame)
estimated_beta =
    param_list_HB[param_list_HB[:,3] .== maximum(param_list_HB[:,3]),1:2]
max_beta1 = maximum(estimated_beta[:,1])
min_beta1 = minimum(estimated_beta[:,1])
max_beta2 = maximum(estimated_beta[:,2])
min_beta2 = minimum(estimated_beta[:,2])
# maxbeta
data = givemedata2(
    data = data_HB_different_coutry,
    sd_err = 1.0,
    true_β = [max_beta1, max_beta2],
    random_seed = 1
    )
data = 
    DataFramesMeta.@chain data begin
    DataFramesMeta.@select :At :Bt :seller_lat :seller_lon :Ab :Bb :buyer_lat :buyer_lon
end  
rename!(data, [:seller_operator_age_normalized, :seller_cumsum_TEU_normalized, :seller_lat, :seller_lon, :buyer_operator_age_normalized, :buyer_cumsum_TEU_normalized, :buyer_lat, :buyer_lon])
filename = "../container_merger_data/output/counterfactual_HB_max_beta.csv"
CSV.write(filename, data)
# minbeta
data = givemedata2(
    data = data_HB_different_coutry,
    sd_err = 1.0,
    true_β = [min_beta1, min_beta2],
    random_seed = 1
    )
data = 
    DataFramesMeta.@chain data begin
    DataFramesMeta.@select :At :Bt :seller_lat :seller_lon :Ab :Bb :buyer_lat :buyer_lon
end  
rename!(data, [:seller_operator_age_normalized, :seller_cumsum_TEU_normalized, :seller_lat, :seller_lon, :buyer_operator_age_normalized, :buyer_cumsum_TEU_normalized, :buyer_lat, :buyer_lon])
filename = "../container_merger_data/output/counterfactual_HB_min_beta.csv"
CSV.write(filename, data)
