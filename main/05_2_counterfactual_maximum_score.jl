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
distance_HB =
    compute_distance_from_lat_long.(
        data_HB.seller_lat,
        data_HB.buyer_lat,
        data_HB.seller_lon,
        data_HB.buyer_lon
    )
data_IHS_different_coutry = 
    hcat(data_IHS, distance_IHS)
data_HB_different_coutry = 
    hcat(data_HB, distance_HB)
# data_IHS_different_coutry =
#     DataFramesMeta.@chain data_IHS_different_coutry begin
#         DataFramesMeta.@subset (:x1 .> 1e-6)
#     end
# data_HB_different_coutry =
#     DataFramesMeta.@chain data_HB_different_coutry begin
#         DataFramesMeta.@subset (:x1 .> 1e-6)
#     end
param_list_IHS =
    CSV.read("../container_merger_data/output/param_list_IHS.csv", DataFrame)
param_list_HB =
    CSV.read("../container_merger_data/output/param_list_HB.csv", DataFrame)

function generate_counterfactual_matching(;
    target_param_list = param_list_IHS,
    target_data = data_IHS_different_coutry,
    text_target_data = "IHS",
    simulation_num = 10
    )
    # load data and param
    estimated_beta =
        target_param_list[target_param_list[:,3] .== maximum(target_param_list[:,3]),1:2]
    max_beta1 = maximum(estimated_beta[:,1])
    min_beta1 = minimum(estimated_beta[:,1])
    max_beta2 = maximum(estimated_beta[:,2])
    min_beta2 = minimum(estimated_beta[:,2])
    # maxbeta
    for k = 1:simulation_num
        data = givemedata2(
        data = target_data,
        sd_err = 1.0,
        true_β = [max_beta1, max_beta2],
        random_seed = k,
        counterfactual_distance = true
        )
        data = 
            DataFramesMeta.@chain data begin
            DataFramesMeta.@select :At :Bt :seller_lat :seller_lon :Ab :Bb :buyer_lat :buyer_lon
            DataFramesMeta.@transform :simulation_id = k
        end 
        if k ==1
            global old_data = data
        else
            global old_data = vcat(old_data, data)
        end 
    end
    data = old_data
    rename!(
        data, 
        [:seller_operator_age_normalized,
         :seller_cumsum_TEU_normalized,
         :seller_lat, :seller_lon, 
         :buyer_operator_age_normalized, 
         :buyer_cumsum_TEU_normalized, 
         :buyer_lat, :buyer_lon,
         :simulation_id]
         )
    filename = "../container_merger_data/output/counterfactual_"*String(text_target_data)*"_max_beta.csv"
    CSV.write(filename, data)
    
    for k = 1:simulation_num
        data = givemedata2(
        data = target_data,
        sd_err = 1.0,
        true_β = [max_beta1, max_beta2],
        random_seed = k,
        counterfactual_distance = false
        )
        data = 
            DataFramesMeta.@chain data begin
            DataFramesMeta.@select :At :Bt :seller_lat :seller_lon :Ab :Bb :buyer_lat :buyer_lon
            DataFramesMeta.@transform :simulation_id = k
        end 
        if k ==1
            global old_data = data
        else
            global old_data = vcat(old_data, data)
        end 
    end
    data = old_data
    rename!(
        data, 
        [:seller_operator_age_normalized,
         :seller_cumsum_TEU_normalized,
         :seller_lat, :seller_lon, 
         :buyer_operator_age_normalized, 
         :buyer_cumsum_TEU_normalized, 
         :buyer_lat, :buyer_lon,
         :simulation_id]
         )
    filename = "../container_merger_data/output/predicted_"*String(text_target_data)*"_max_beta.csv"
    CSV.write(filename, data)

    # minbeta
    for k = 1:simulation_num
        data = givemedata2(
        data = target_data,
        sd_err = 1.0,
        true_β = [min_beta1, min_beta2],
        random_seed = k,
        counterfactual_distance = true
        )
        data = 
            DataFramesMeta.@chain data begin
            DataFramesMeta.@select :At :Bt :seller_lat :seller_lon :Ab :Bb :buyer_lat :buyer_lon
            DataFramesMeta.@transform :simulation_id = k
        end 
        if k ==1
            global old_data = data
        else
            global old_data = vcat(old_data, data)
        end 
    end
    data = old_data
    rename!(
        data, 
        [:seller_operator_age_normalized,
         :seller_cumsum_TEU_normalized,
         :seller_lat, :seller_lon, 
         :buyer_operator_age_normalized, 
         :buyer_cumsum_TEU_normalized, 
         :buyer_lat, :buyer_lon,
         :simulation_id]
         )
    filename = "../container_merger_data/output/counterfactual_"*String(text_target_data)*"_min_beta.csv"
    CSV.write(filename, data)

    for k = 1:simulation_num
        data = givemedata2(
        data = target_data,
        sd_err = 1.0,
        true_β = [min_beta1, min_beta2],
        random_seed = k,
        counterfactual_distance = false
        )
        data = 
            DataFramesMeta.@chain data begin
            DataFramesMeta.@select :At :Bt :seller_lat :seller_lon :Ab :Bb :buyer_lat :buyer_lon
            DataFramesMeta.@transform :simulation_id = k
        end 
        if k ==1
            global old_data = data
        else
            global old_data = vcat(old_data, data)
        end 
    end
    data = old_data
    rename!(
        data, 
        [:seller_operator_age_normalized,
         :seller_cumsum_TEU_normalized,
         :seller_lat, :seller_lon, 
         :buyer_operator_age_normalized, 
         :buyer_cumsum_TEU_normalized, 
         :buyer_lat, :buyer_lon,
         :simulation_id]
         )
    filename = "../container_merger_data/output/predicted_"*String(text_target_data)*"_min_beta.csv"
    CSV.write(filename, data)
end

@time generate_counterfactual_matching(
    target_param_list = param_list_IHS,
    target_data = data_IHS_different_coutry,
    text_target_data = "IHS",
    simulation_num = 100
    )

# HB
@time generate_counterfactual_matching(
    target_param_list = param_list_HB,
    target_data = data_HB_different_coutry,
    text_target_data = "HB",
    simulation_num = 100
    )