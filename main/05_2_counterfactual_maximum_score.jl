#---------------#
# Counterfactual
#---------------#
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
