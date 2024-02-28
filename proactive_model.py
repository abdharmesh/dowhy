from dowhy import gcm
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from utility_func import generate_graph
gcm.config.disable_progress_bars()

#class of Proactive Model
class Proactive_model:
    def __init__(self , type_ , total_data):
        self.causal_graph = generate_graph(type_ = type_)
        self.causal_model = gcm.StructuralCausalModel(self.causal_graph) 
        gcm.auto.assign_causal_mechanisms(self.causal_model , total_data)


    def view_graph(self):
        gcm.util.plot(self.causal_graph , figure_size = [10,10])
    

    
    def change_in_value(self , target_node , normal_data , outlier_data):
        t = target_node
        value_t = outlier_data[t].mean()
        value_t_1 = normal_data[t].mean()
        delta = 100.0*(value_t - value_t_1)/value_t_1
        if value_t >= value_t_1:
            print("Your " ,t, " increased by " ,round(delta,1), " percent")
        else:
            print("Your " ,t ," decreased by " ,round(delta,1), " percent")



    def generate_attributes(self ,normal_data , outlier_data , target_node):
        t = target_node
        value_t = outlier_data[t].mean()
        value_t_1 = normal_data[t].mean()
        delta = 100.0*(value_t - value_t_1)/value_t_1
        if value_t >= value_t_1:
            print("Your " ,t, " increased by " ,round(delta,1), " percent")
        else:
            print("Your " ,t ," decreased by " ,round(delta,1), " percent")


        gcm.fit(self.causal_model , normal_data)
        attributions = gcm.attribute_anomalies(self.causal_model , target_node = target_node , anomaly_samples = outlier_data)
        impact_nodes = []
        impact = []
        for k in attributions:
            if attributions[k][0] > 0:
                impact_nodes.append(k)
                impact.append(attributions[k][0])

        total_sum = sum(impact)
        for i in range(len(impact)):
            impact[i] = round(100.0*impact[i]/total_sum ,1)


        df = pd.DataFrame({'impact_nodes' : impact_nodes , 'impact' : impact})
        df_sorted = df.sort_values(by="impact", ascending=False)


        print("Following are the reasons with contribution : ")
        # Iterate through the sorted DataFrame
        for index, row in df_sorted.iterrows():
            node = row['impact_nodes']
            value_t = outlier_data[node].mean()
            value_t_1 = normal_data[node].mean()
            delta = 100.0*(value_t - value_t_1)/value_t_1
            if node == target_node:
                print("Your unobserved confounders" , " has contribution of ",row['impact'] , " percent")
            else:
                if value_t >= value_t_1:
                    print("Your " ,node, " increased by " ,round(delta,1), " percent and has contribution of ",row['impact'] , " percent")
                else:
                    print("Your " ,node," decreased by " ,round(delta,1), " percent and has contribution of ",row['impact'] , " percent")
        
        df['impact_nodes'] = df['impact_nodes'].replace(target_node ,'Unobserved_confounders')
        plt.figure(figsize = (10,5))
        plt.bar(df['impact_nodes'] , df['impact'])
        # Add labels
        plt.xlabel("impact_nodes")
        plt.ylabel("contribution")

        # Add title
        plt.title(f"Reasons for change in {target_node}")
        plt.show()


    def get_arrow_strength(self , target_node):
        strength = gcm.arrow_strength(self.causal_model, 'orders')
        return strength
    
    def dist_change_confidence_interval(self , target_node , normal_data , outlier_data):
        median_contribs, uncertainty_contribs = gcm.confidence_intervals(
            gcm.bootstrap_sampling(gcm.distribution_change,
                                self.causal_model,
                                normal_data,
                                outlier_data,
                                target_node,
                                num_samples=2000,
                                difference_estimation_func=lambda x1, x2 : np.mean(x2) - np.mean(x1)),
            confidence_level=0.95,
            num_bootstrap_resamples=10)

        yerr_plus = [uncertainty_contribs[node][1] - median_contribs[node] for node in median_contribs.keys()]
        yerr_minus = [median_contribs[node] - uncertainty_contribs[node][0] for node in median_contribs.keys()]
        plt.bar(median_contribs.keys(), median_contribs.values(), yerr=np.array([yerr_minus, yerr_plus]), ecolor='black')
        plt.ylabel('Contribution')
        plt.show()

    
    def distribution_change(self , target_node , normal_data , outlier_data):
        contributions = gcm.distribution_change(self.causal_model,
                                        normal_data,
                                        outlier_data,
                                        target_node,
                                        num_samples= normal_data.shape[0],
                                        difference_estimation_func=lambda x1, x2 : np.mean(x2) - np.mean(x1))
        plt.figure(figsize=(20, 5))
        plt.bar(contributions.keys(), contributions.values())
        plt.ylabel('Contribution')
        plt.show()

    
    def attribute_confidence_interval(self , target_node , normal_data , outlier_data):
        median_attribs , uncertainity_attribs = gcm.confidence_intervals(gcm.fit_and_compute(gcm.attribute_anomalies ,
        self.causal_model,normal_data,target_node=target_node,anomaly_samples=outlier_data),num_bootstrap_resamples=10)
        gcm.util.bar_plot(median_attribs , 'Attribution Score')
