import os
import boto3
import urllib.parse
from sample_sheet import SampleSheet
import sample_sheet
s3 = boto3.client('s3')

'''
def get_experiment_name(experiment_line):
    comma_index_list=[c[0] for c in enumerate(experiment_line) if c[1]==',']
    experiment_name = experiment_line[comma_index_list[0]+1 : comma_index_list[1]]
    return experiment_name
'''

def save_to_s3_bucket(tmp_dir):
    temp_name = os.path.basename(tmp_dir)
    s3 = boto3.resource('s3')
    bucket = "ncgm-data-store-and-automation"
    prefix = "hamming_distance/Output/"
    dir = prefix + temp_name
    print("inside save to s3 bucket")
    print(dir)
    s3.meta.client.upload_file(tmp_dir, bucket , dir)
    print("Transfer complete to s3 bucket output folder ")

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        experiment_name = os.path.splitext(os.path.basename(key))[0]
        response = s3.get_object(Bucket=bucket, Key=key)
        temp = open("/tmp/temp.csv",'w')
        text = response["Body"].read().split(b'\n')
        for line in text:
            line_string = line.decode("utf-8")
            temp.write(line_string+"\n")
        temp.close()
        main_function("/tmp/temp.csv",experiment_name)
        return response['ContentType']
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def calculate_hamming_distance(index1, index2):
    """
    Different Nucleotide on each index for two Strings
    :param first_string: index1
    :type first_string: string
    :param second_string: index2
    :type second_string: string
    """
    distance = 0
    for counter_x, counter_y in zip(index1, index2):
        if counter_x != counter_y:
            distance += 1
    return distance

def maximum_degree(adjacency_matrix):
    """
    node with max degree , will determine the number of clusters of group

    :param adjacency_matrix: vertices with edges pointing to other vertices
    :type adjacency_matrix: graph
    :return: max degree node
    :rtype: integer
    """
    max_degree=-1
    for adjacent_vertices in adjacency_matrix.values():
        max_degree=max(max_degree,len(adjacent_vertices))
    return max_degree

def generate_hamming_distance_data_and_split(samplesheet_file, experiment_name):
    """
    hamming distance data
    :param samplesheet: sample sheet
    :type samplesheet: CSV
    """
    sample_sheet_object = SampleSheet(os.path.realpath(samplesheet_file))
    number_of_samples = len(sample_sheet_object.samples)
    all_i7_index_values = [sample["index"] for sample in sample_sheet_object.samples]
    all_i5_index_values = [sample["index2"] for sample in sample_sheet_object.samples]
    all_samples_id = [ sample["Sample_ID"] for sample in sample_sheet_object.samples ]
    all_samples_name = [ sample["Sample_Name"] for sample in sample_sheet_object.samples ]
    all_samples_plate = [sample["Sample_Plate"] for sample in sample_sheet_object.samples ]
    all_samples_well = [sample["Sample_Well"] for sample in sample_sheet_object.samples ]
    all_index_plate = [sample["Index_Plate_Well"] for sample in sample_sheet_object.samples ]
    all_i7_index = [sample["I7_Index_ID"] for sample in sample_sheet_object.samples ]
    all_i5_index = [sample["I5_Index_ID"] for sample in sample_sheet_object.samples ]
    all_project = [sample["Sample_Project"] for sample in sample_sheet_object.samples ]
    all_description = [sample["Description"] for sample in sample_sheet_object.samples ]
    if "Lane" in sample_sheet_object.all_sample_keys:
        all_lane_values = [sample["Lane"] for sample in sample_sheet_object.samples]
    else:
        all_lane_values = [0] * number_of_samples
    experiment_name_dir = "/tmp/" + experiment_name + ".tsv"
    hamming_distance_information_file = open(experiment_name_dir,'w')
    hamming_distance_information_file.write(
        "lane\thamming_distance_i7\thamming_distance_i5\tfirst_sample\tsecond_sample\t\
            first_sample_i7_indexes\tsecond_sample_i7_indexes\t\
                first_sample_i5_indexes\tsecond_sample_i5_indexes"
    )
    hamming_distance_information_file.write('\n')
    adjacency_matrix = {}
    save_to_s3_bucket_list=[]
    first_group,second_group = [], []
    some_samples_with_hamming_distances_issue_flag = False
    for counter_x in range(number_of_samples):
        for counter_y in range(counter_x, number_of_samples):
            if counter_x != counter_y:
                sam1_i7 = all_i7_index_values[counter_x]
                sam1_i5 = all_i5_index_values[counter_x]
                sam2_i7 = all_i7_index_values[counter_y]
                sam2_i5 = all_i5_index_values[counter_y]
                dis1 = calculate_hamming_distance(sam1_i7, sam2_i7)
                dis2 = calculate_hamming_distance(sam1_i5, sam2_i5)
                sam1_lane_value = all_lane_values[counter_x]
                sam2_lane_value = all_lane_values[counter_y]
                if sam1_lane_value == sam2_lane_value and dis1 <= 2 and dis2 <= 2:
                    some_samples_with_hamming_distances_issue_flag=True
                    first_group.append(counter_x + 1)
                    second_group.append(counter_y + 1)
                    if counter_x + 1 not in [key for key,value in adjacency_matrix.items()]:
                        adjacency_matrix[counter_x + 1]=[counter_y + 1]
                    else:
                        adjacency_matrix[counter_x + 1].append(counter_y + 1)
                    if counter_y + 1 not in [key for key,value in adjacency_matrix.items()]:
                        adjacency_matrix[counter_y + 1]=[counter_x + 1]
                    else:
                        adjacency_matrix[counter_y + 1].append(counter_x + 1)
                    hamming_distance_information_file.write(
                        f"{sam1_lane_value}\t{dis1}\t{dis2}\t{counter_x+1}\t\
                            {counter_y+1}\t{sam1_i7}\t{sam2_i7}\t{sam1_i5}\t{sam2_i5}"
                    )
                    hamming_distance_information_file.write('\n')
    hamming_distance_information_file.close()
    save_to_s3_bucket_list.append(experiment_name_dir)
    if some_samples_with_hamming_distances_issue_flag:
        connected_components = set()
        #dis_joint_elements =  []
        numbers_of_clusters = maximum_degree(adjacency_matrix)
        clusters = {(counter + 1) : set() for counter in range(numbers_of_clusters + 1)}
        nodes = sorted ( list ( set ( sorted(first_group) + sorted(second_group) ) ) )
        nodes.reverse()
        while len(nodes)>0:
            vertex = nodes[len(nodes)-1]
            if vertex not in connected_components:
                boolean_flag_list = [False]
                for counter in range(numbers_of_clusters+1):
                    boolean_flag_list.append(True)
                for vertices in adjacency_matrix[vertex]:
                    for cluster_key, cluster_value in clusters.items():
                        if vertices in clusters[cluster_key]:
                            boolean_flag_list[cluster_key]=False
                for index in enumerate(boolean_flag_list):
                    if boolean_flag_list[index[0]]:
                        nodes.pop()
                        clusters[index[0]].add( vertex )
                        connected_components.add( vertex )
                        break
        separate_samples=[]
        for separate_sample in range(number_of_samples):
            if separate_sample + 1 not in connected_components:
                separate_samples.append(separate_sample + 1)

        for cluster_key, cluster_values in clusters.items():
            if cluster_values:
                while len(separate_samples)>0:
                    cluster_values.add(separate_samples.pop())
                new_sample_sheet = SampleSheet()
                new_sample_sheet.Header = sample_sheet_object.Header
                new_sample_sheet.Reads = sample_sheet_object.Reads
                new_sample_sheet.Settings = sample_sheet_object.Settings
                for cluster_value in cluster_values:
                    cluster_value-=1
                    sample_values=[all_lane_values[cluster_value],
                            all_samples_id[cluster_value],
                            all_samples_name[cluster_value],
                            all_samples_plate[cluster_value],
                            all_samples_well[cluster_value],
                            all_index_plate[cluster_value],
                            all_i7_index[cluster_value],
                            all_i7_index_values[cluster_value],
                            all_i5_index[cluster_value],
                            all_i5_index_values[cluster_value],
                            all_project[cluster_value],
                            all_description[cluster_value]]
                    sample = dict( zip ( sample_sheet_object.all_sample_keys, sample_values ) )
                    sample = sample_sheet.Sample(sample)
                    new_sample_sheet.add_sample(sample)
                new_sample_sheet_file_name = "_".join([experiment_name,str(cluster_key)]) + ".csv"
                temp_name = "/tmp/" + new_sample_sheet_file_name
                new_sample_sheet.write( open ( temp_name, 'w' , encoding = "utf8" ) )
                save_to_s3_bucket_list.append(temp_name)
    return save_to_s3_bucket_list

def main_function(samplesheet_file, experiment_name):
    s3_list = generate_hamming_distance_data_and_split(samplesheet_file, experiment_name)
    for file in s3_list:
        save_to_s3_bucket(file)
