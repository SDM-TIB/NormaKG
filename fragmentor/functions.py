import re
import csv
import sys
import os
import pandas as pd

global prefixes
prefixes = {}

def is_key_unique(csv_file, key_column):
    seen = set()
    with open(csv_file, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row[key_column]
            if key in seen:
                return False
            seen.add(key)
    return True

def type_join(parent_source, child_source, parent, child):

    is_parent_unique = is_key_unique(parent_source,parent)
    is_child_unique = is_key_unique(child_source,child)

    if is_parent_unique and is_child_unique:
        return True
    else:
        return False

def join_data_sources(child, parent, child_source, parent_source, child_properties, parent_properties, output):

    with open(child_source, mode='r', newline='') as child_file:
        with open(parent_source, mode='r', newline='') as parent_file:
            with open(output, mode='w', newline='') as output_file:
                writer = csv.writer(output_file)
                child_data = csv.DictReader(child_file, delimiter=',')
                parent_data = csv.DictReader(parent_file, delimiter=',')
                writer.writerow(child_properties + parent_properties)
                output_values = {}
                parent_values = {}
                join_parent = {}

                for row in parent_data:
                    if parent in row:
                        if row[parent] != "" and row[parent] != None:
                            subject_values = []
                            subject_string = ""
                            for attr in parent_properties:
                                subject_values.append(row[attr])
                                subject_string += row[attr] + "_"
                            subject_string += row[parent]
                            if subject_string not in parent_values:
                                parent_values[subject_string] = ""
                                if row[parent] in join_parent:
                                    join_parent[row[parent]].append(subject_values)
                                else:
                                    join_parent[row[parent]] = [subject_values]
                parent_values = {}     
                for row in child_data:
                    if child in row:
                        if row[child] != "" and row[child] != None:
                            if row[child] in join_parent:
                                child_string = ""
                                child_row = []
                                for attr in child_properties:
                                    child_row.append(row[attr])
                                    child_string += row[attr] + "_"
                                child_string += row[child]
                                if child_string not in output_values:
                                    output_values[child_string] = ""
                                    for parent_row in join_parent[row[child]]:
                                        final_row = child_row + parent_row
                                        writer.writerow(final_row)

def config_writer(engine, output, output_name, mapping):
    if "SDM-RDFizer" == engine:
        config_file = "[default]\nmain_directory: "
        config_file += output + "\n\n"
        config_file += "[datasets]\nnumber_of_datasets: 1\noutput_folder: ${default:main_directory}\nremove_duplicate: yes\nall_in_one_file: no\nordered: yes\nenrichment: yes\nname: "
        config_file += output_name + "\n"
        """if "dbtype" in original_config:
            config_file += "dbtype: " + original_config["dbtype"] + "\n"""
        config_file += "\n"
        config_file += "[dataset1]\nname: "
        config_file += mapping.split(".")[0] + "\n"
        config_file += "mapping: " + output + "/" + mapping + "\n"
        """if "dbtype" in original_config:
            config_file += "user: " + original_config["user"] + "\n"
            config_file += "password: " + original_config["password"] + "\n"
            config_file += "host: " + original_config["host"] + "\n"
            config_file += "port: " + original_config["port"] + "\n"
            config_file += "db: " + original_config["db"] + "\n"""
        config_name = output + "/" + "configfile_" + output_name + ".ini"
    elif "RocketRML" == engine:
        config_file = "const parser = require('rocketrml');\nconst doMapping = async () => {\n  const options = {\n    toRDF: true,\n"
        config_file += "    verbose: true,\n    xmlPerformanceMode: false,\n    replace: false,\n  };\n"
        config_file += "  const result = await parser.parseFile(\'"
        config_file += output + "/" + mapping + "\', \'" + output + "/" + output_name + ".nt\', options).catch((err) => { console.log(err); });\n"
        config_file += "  console.log(result);\n};\n\ndoMapping();"
        config_name = output + "/" + "configfile_" + output_name + ".js"
    elif "Morph-KGC" == engine:
        config_file = "[CONFIGURATION]\n\n# OUTPUT\noutput_file="
        config_file += output + "/" + output_name + ".nt\n"
        config_file += "output_format=N-TRIPLES\n\n[DataSource1]\nsource_type=CSV\nmappings="
        config_file += output + "/" + mapping
        config_name = output + "/" + "configfile_" + output_name + ".ini"
    mapping_file = open(config_name,"w")
    mapping_file.write(config_file)
    mapping_file.close()
    return config_name

def execute_engines(engine, mapping, output, output_name):
    if "Morph-KGC" == engine:
        command += "python3 -m morph_kgc " + config_writer(engine, output, output_name, mapping)
    else:
        command = ""
        if "SDM-RDFizer" == engine:
            command = "python3 -m rdfizer -c " + config_writer(engine, output, output_name, mapping)
        elif "RMLMapper" == engine:
            command = "java -jar rmlmapper.jar -d -m "
            command = output + "/" + mapping + " -o "
            command = output + "/" + output_name + ".nt"
        elif "RocketRML" == engine:
            command = "node --max-old-space-size=32000 " + config_writer(engine, output, output_name, mapping)
        elif "FlexRML" == engine:
            command = "./flexrml -m " + output + "/" + mapping  + " -o " + output + "/" + output_name + ".nt"
        else:
            print("The " + config["datasets"]["engine"] + " is not a compatible tool.")
            print("Aborting...")
            sys.exit(1)
        os.system(command)

def fd_determination(subject_attr,po_attr,fd):
    for attr in subject_attr:
        for po in po_attr:
            if po not in subject_attr:
                if po in fd:
                    if attr not in fd[po]:
                        return False
                else:
                    return False
    return True

def string_separetion(string):
    if ("{" in string) and ("[" in string):
        prefix = string.split("{")[0]
        condition = string.split("{")[1].split("}")[0]
        postfix = string.split("{")[1].split("}")[1]
        field = prefix + "*" + postfix
    elif "[" in string:
        return string, string
    else:
        return string, ""
    return string, condition

def extract_attr(value,attr_list):
    pos_attr = value.split("{")
    for attr in pos_attr:
        if "}" in attr:
            if attr.split("}")[0] not in attr_list:
                attr_list.append(attr.split("}")[0])
    return attr_list

def prefix_extraction(original, uri):
    url = ""
    value = ""
    if prefixes:
        if "#" in uri:
            url, value = uri.split("#")[0]+"#", uri.split("#")[1]
        else:
            value = uri.split("/")[len(uri.split("/"))-1]
            char = ""
            temp = ""
            temp_string = uri
            while char != "/":
                temp = temp_string
                temp_string = temp_string[:-1]
                char = temp[len(temp)-1]
            url = temp
    else:
        f = open(original,"r")
        original_mapping = f.readlines()
        for prefix in original_mapping:
            if ("prefix" in prefix) or ("base" in prefix):
                elements = prefix.split(" ")
                elements[2] = elements[2].replace(" ","")
                elements[2] = elements[2].replace("\n","")
                if ">" in elements[2].replace(" ","")[1:-1]:
                    prefixes[elements[2].replace(" ","")[1:-2]] = elements[1][:-1]
                else:
                    prefixes[elements[2].replace(" ","")[1:-1]] = elements[1][:-1]
            else:
                break
        f.close()
        if "#" in uri:
            url, value = uri.split("#")[0]+"#", uri.split("#")[1]
        else:
            value = uri.split("/")[len(uri.split("/"))-1]
            char = ""
            temp = ""
            temp_string = uri
            while char != "/":
                temp = temp_string
                temp_string = temp_string[:-1]
                char = temp[len(temp)-1]
            url = temp
    if url in prefixes:
        return prefixes[url], url, value
    else:
        return None,None,None

def functional_projection(original, output_folder, output_name, triples_map_list, fd, engine):
    mapping = ""
    parent_triples_maps = {}
    child_triples_maps = {}
    proyections = {}
    non_func = {}

    for triples_map in triples_map_list:
        print(triples_map.triples_map_id)
        subject_attr = []
        if len(triples_map.predicate_object_maps_list) > 0 and triples_map.predicate_object_maps_list[0].predicate_map.value != "None":
            if triples_map.data_source in proyections:
                source = output_folder + "/" + triples_map.data_source.split(".")[0].split("/")[len(triples_map.data_source.split(".")[0].split("/"))-1] + "_proyected_"+ str(len(proyections[triples_map.data_source])+1) +".csv"
                proyections[triples_map.data_source][source] = []
            else:
                source = output_folder + "/" + triples_map.data_source.split(".")[0].split("/")[len(triples_map.data_source.split(".")[0].split("/"))-1] + "_proyected_1.csv"
                proyections[triples_map.data_source] = {source:[]}
        else:
            source = triples_map.data_source
        mapping += "<" + triples_map.triples_map_id + ">\n"
        mapping += "    a rr:TriplesMap;\n"
        if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None":
            mapping += "    rml:logicalSource [ rml:source \"" + source +"\";\n"
            mapping += "                rml:referenceFormulation ql:CSV\n"
        mapping += "                ];\n"

        mapping += "    rr:subjectMap [\n"
        if "template" in triples_map.subject_map.subject_mapping_type:
            subject_attr = extract_attr(triples_map.subject_map.value, subject_attr)
            if len(triples_map.predicate_object_maps_list) > 0 and triples_map.predicate_object_maps_list[0].predicate_map.value != "None":
                proyections[triples_map.data_source][source] = extract_attr(triples_map.subject_map.value, proyections[triples_map.data_source][source])
            mapping += "        rr:template \"" + triples_map.subject_map.value + "\";\n"
        elif "reference" in triples_map.subject_map.subject_mapping_type:
            subject_attr.append(triples_map.subject_map.value)
            if len(triples_map.predicate_object_maps_list) > 0 and triples_map.predicate_object_maps_list[0].predicate_map.value != "None":
                if triples_map.subject_map.value not in proyections[triples_map.data_source][source]:
                    proyections[triples_map.data_source][source].append(triples_map.subject_map.value)
            mapping += "        rml:reference \"" + triples_map.subject_map.value + "\";\n"
            mapping += "        rr:termType rr:IRI\n"
        elif "constant" in triples_map.subject_map.subject_mapping_type:
            mapping += "        rr:constant \"" + triples_map.subject_map.value + "\";\n"
            mapping += "        rr:termType rr:IRI\n"
        if triples_map.subject_map.rdf_class[0] != None:
            prefix, url, value = prefix_extraction(original, triples_map.subject_map.rdf_class[0])
            mapping += "        rr:class " + prefix + ":" + value  + "\n"
        mapping += "    ];\n"

        if len(triples_map.predicate_object_maps_list) > 0 and triples_map.predicate_object_maps_list[0].predicate_map.value != "None":
            for predicate_object in triples_map.predicate_object_maps_list:
                func = True
                if "template" in predicate_object.object_map.mapping_type:
                    object_attr = extract_attr(predicate_object.object_map.value,[])
                    func = fd_determination(subject_attr,object_attr,fd)
                elif "reference" == predicate_object.object_map.mapping_type:
                    func = fd_determination(subject_attr,[predicate_object.object_map.value],fd)
                if not func:
                    if triples_map.triples_map_id in non_func:
                        non_func[triples_map.triples_map_id][predicate_object.predicate_map.value] = ""
                    else:
                        non_func[triples_map.triples_map_id] = {predicate_object.predicate_map.value:""}
                if "parent triples map" not in predicate_object.object_map.mapping_type:
                    if func:
                        mapping += "    rr:predicateObjectMap [\n"
                        if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                            prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                            mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                        elif "constant" in predicate_object.predicate_map.mapping_type :
                            prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                            if prefix != None:
                                mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                            else:
                                mapping += "        rr:predicateMap[\n"
                                mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                mapping += "        ];\n"
                        elif "template" in predicate_object.predicate_map.mapping_type:
                            mapping += "        rr:predicateMap[\n"
                            mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                            mapping += "        ];\n"
                        elif "reference" in predicate_object.predicate_map.mapping_type:
                            mapping += "        rr:predicateMap[\n"
                            mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                            mapping += "        ];\n"

                        mapping += "        rr:objectMap "
                        if "constant" in predicate_object.object_map.mapping_type:
                            mapping += "[\n"
                            mapping += "            rr:constant \"" + predicate_object.object_map.value + "\"\n"
                            mapping += "        ]\n"
                        elif "template" in predicate_object.object_map.mapping_type:
                            proyections[triples_map.data_source][source] = extract_attr(predicate_object.object_map.value,proyections[triples_map.data_source][source])
                            mapping += "[\n"
                            mapping += "            rr:template  \"" + predicate_object.object_map.value + "\"\n"
                            mapping += "        ]\n"
                        elif "reference" == predicate_object.object_map.mapping_type:
                            if predicate_object.object_map.value not in proyections[triples_map.data_source][source]:
                                proyections[triples_map.data_source][source].append(predicate_object.object_map.value)
                            mapping += "[\n"
                            mapping += "            rml:reference \"" + predicate_object.object_map.value + "\"\n"
                            if predicate_object.object_map.datatype is not None:
                                prefix, url, value = prefix_extraction(original, predicate_object.object_map.datatype)
                                mapping = mapping[:-1]
                                mapping += ";\n            rr:datatype " + prefix + ":" + value + ";\n"
                            elif predicate_object.object_map.term is not None:
                                prefix, url, value = prefix_extraction(original, predicate_object.object_map.term)
                                mapping = mapping[:-1]
                                mapping += ";\n            rr:termType " + prefix + ":" + value + ";\n"
                            mapping += "        ]\n"
                        elif "constant shortcut" in predicate_object.object_map.mapping_type:
                            mapping += "[\n"
                            mapping += "            rr:constant \"" + predicate_object.object_map.value + "\"\n"
                            mapping += "        ]\n"
                        mapping += "    ];\n"
                else:
                    for tm in triples_map_list:
                        if tm.triples_map_id == predicate_object.object_map.value:
                            if tm.data_source == triples_map.data_source:
                                if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                                    if predicate_object.object_map.child == predicate_object.object_map.parent:
                                        if type_join(tm.data_source, triples_map.data_source, predicate_object.object_map.parent[0], predicate_object.object_map.child[0]):
                                            object_attr = extract_attr(tm.subject_map.value,[])
                                            if fd_determination(subject_attr,object_attr,fd):
                                                mapping += "    rr:predicateObjectMap [\n"
                                                if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                                                    prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                    mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                                elif "constant" in predicate_object.predicate_map.mapping_type :
                                                    prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                    if prefix != None:
                                                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                                    else:
                                                        mapping += "        rr:predicateMap[\n"
                                                        mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                                        mapping += "        ];\n"
                                                elif "template" in predicate_object.predicate_map.mapping_type:
                                                    mapping += "        rr:predicateMap[\n"
                                                    mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                                                    mapping += "        ];\n"
                                                elif "reference" in predicate_object.predicate_map.mapping_type:
                                                    mapping += "        rr:predicateMap[\n"
                                                    mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                                                    mapping += "        ];\n"
                                                proyections[triples_map.data_source][source] = extract_attr(tm.subject_map.value,proyections[triples_map.data_source][source])
                                                mapping += "        rr:objectMap [\n"
                                                mapping += "            rr:template  \"" + tm.subject_map.value  + "\"\n"
                                                mapping += "        ]\n"
                                                mapping += "    ];\n"
                                            else:
                                                if triples_map.triples_map_id in non_func:
                                                    non_func[triples_map.triples_map_id][predicate_object.predicate_map.value] = ""
                                                else:
                                                    non_func[triples_map.triples_map_id] = {predicate_object.predicate_map.value:""}
                                        else:
                                            mapping += "    rr:predicateObjectMap [\n"
                                            if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                                                prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                            elif "constant" in predicate_object.predicate_map.mapping_type :
                                                prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                if prefix != None:
                                                    mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                                else:
                                                    mapping += "        rr:predicateMap[\n"
                                                    mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                                    mapping += "        ];\n"
                                            elif "template" in predicate_object.predicate_map.mapping_type:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                            elif "reference" in predicate_object.predicate_map.mapping_type:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                            mapping += "        rr:objectMap [\n"
                                            mapping += "            rr:template \"" + tm.subject_map.value + "\"\n"
                                            object_attr = extract_attr(tm.subject_map.value,[])
                                            join_data_sources(predicate_object.object_map.child[0], predicate_object.object_map.parent[0], 
                                                            triples_map.data_source, tm.data_source, 
                                                            subject_attr, object_attr, source)
                                            mapping += "        ]\n"    
                                            mapping += "    ];\n"

                                    else:
                                        if len(triples_map.predicate_object_maps_list) > 1:
                                            if triples_map.triples_map_id not in child_triples_maps:
                                                child_triples_maps[triples_map.triples_map_id] = triples_map
                                        else:
                                            if tm.triples_map_id == predicate_object.object_map.value:
                                                mapping += "    rr:predicateObjectMap [\n"
                                                if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                                                    prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                    mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                                elif "constant" in predicate_object.predicate_map.mapping_type :
                                                    prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                    if prefix != None:
                                                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                                    else:
                                                        mapping += "        rr:predicateMap[\n"
                                                        mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                                        mapping += "        ];\n"
                                                elif "template" in predicate_object.predicate_map.mapping_type:
                                                    mapping += "        rr:predicateMap[\n"
                                                    mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                                                    mapping += "        ];\n"
                                                elif "reference" in predicate_object.predicate_map.mapping_type:
                                                    mapping += "        rr:predicateMap[\n"
                                                    mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                                                    mapping += "        ];\n"
                                                mapping += "        rr:objectMap [\n"
                                                if tm.predicate_object_maps_list[0].predicate_map.value == "None":
                                                    mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+ ">;\n"
                                                else:
                                                    if type_join(tm.data_source,triples_map.data_source,predicate_object.object_map.parent[0],predicate_object.object_map.child[0]):
                                                        if predicate_object.object_map.child[0] not in proyections[triples_map.data_source][source]:
                                                            proyections[triples_map.data_source][source].append(predicate_object.object_map.child[0])
                                                        if predicate_object.object_map.value in parent_triples_maps:
                                                            parent = str(len(parent_triples_maps[predicate_object.object_map.value])+1)
                                                            parent_triples_maps[predicate_object.object_map.value][predicate_object.object_map.value+"_parent_"+parent] = predicate_object.object_map.parent[0]
                                                            mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value +"_parent_" + parent + ">;\n"
                                                        else:
                                                            parent_triples_maps[predicate_object.object_map.value] = {predicate_object.object_map.value+"_parent_1":predicate_object.object_map.parent[0]}
                                                            mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+"_parent_1" + ">;\n"
                                                        mapping += "        rr:joinCondition [\n"
                                                        mapping += "            rr:child \"" + predicate_object.object_map.child[0] + "\";\n"
                                                        mapping += "            rr:parent \"" + predicate_object.object_map.parent[0] + "\";\n"
                                                        mapping += "            ]\n"
                                                        mapping += "            ]\n"
                                                    else:
                                                        value = proyections.pop(triples_map.data_source, None)
                                                        mapping += "            rr:template \"" + tm.subject_map.value + "\"\n"
                                                        object_attr = extract_attr(tm.subject_map.value,[])
                                                        join_data_sources(predicate_object.object_map.child[0], predicate_object.object_map.parent[0], 
                                                                        triples_map.data_source, tm.data_source, 
                                                                        subject_attr, object_attr, source)
                                                        mapping += "        ]\n"    
                                                mapping += "        ];\n"

                                else:
                                    object_attr = extract_attr(tm.subject_map.value,[])
                                    if fd_determination(subject_attr,object_attr,fd):
                                        mapping += "    rr:predicateObjectMap [\n"
                                        if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                                            prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                            mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                        elif "constant" in predicate_object.predicate_map.mapping_type :
                                            prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                            if prefix != None:
                                                mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                            else:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                        elif "template" in predicate_object.predicate_map.mapping_type:
                                            mapping += "        rr:predicateMap[\n"
                                            mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                                            mapping += "        ];\n"
                                        elif "reference" in predicate_object.predicate_map.mapping_type:
                                            mapping += "        rr:predicateMap[\n"
                                            mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                                            mapping += "        ];\n"
                                        proyections[triples_map.data_source][source] = extract_attr(tm.subject_map.value,proyections[triples_map.data_source][source])
                                        mapping += "        rr:objectMap [\n"
                                        mapping += "            rr:template  \"" + tm.subject_map.value  + "\"\n"
                                        mapping += "        ]\n"
                                        mapping += "    ];\n"
                                    else:
                                        if triples_map.triples_map_id in non_func:
                                            non_func[triples_map.triples_map_id][predicate_object.predicate_map.value] = ""
                                        else:
                                            non_func[triples_map.triples_map_id] = {predicate_object.predicate_map.value:""}
                            else:
                                if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                                    if len(triples_map.predicate_object_maps_list) > 1:
                                        if triples_map.triples_map_id not in child_triples_maps:
                                            child_triples_maps[triples_map.triples_map_id] = triples_map
                                    else:
                                        if tm.triples_map_id == predicate_object.object_map.value:
                                            mapping += "    rr:predicateObjectMap [\n"
                                            if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                                                prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                            elif "constant" in predicate_object.predicate_map.mapping_type :
                                                prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                if prefix != None:
                                                    mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                                else:
                                                    mapping += "        rr:predicateMap[\n"
                                                    mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                                    mapping += "        ];\n"
                                            elif "template" in predicate_object.predicate_map.mapping_type:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                            elif "reference" in predicate_object.predicate_map.mapping_type:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                            mapping += "        rr:objectMap [\n"
                                            if tm.predicate_object_maps_list[0].predicate_map.value == "None":
                                                mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value + ">;\n"
                                            else:
                                                if type_join(tm.data_source,triples_map.data_source,predicate_object.object_map.parent[0],predicate_object.object_map.child[0]):
                                                    if predicate_object.object_map.child[0] not in proyections[triples_map.data_source][source]:
                                                        proyections[triples_map.data_source][source].append(predicate_object.object_map.child[0])
                                                    if predicate_object.object_map.value in parent_triples_maps:
                                                        parent = str(len(parent_triples_maps[predicate_object.object_map.value])+1)
                                                        parent_triples_maps[predicate_object.object_map.value][predicate_object.object_map.value+"_parent_"+parent] = predicate_object.object_map.parent[0]
                                                        mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value +"_parent_" + parent + ">;\n"
                                                    else:
                                                        parent_triples_maps[predicate_object.object_map.value] = {predicate_object.object_map.value+"_parent_1":predicate_object.object_map.parent[0]}
                                                        mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+"_parent_1" + ">;\n"
                                                    mapping += "        rr:joinCondition [\n"
                                                    mapping += "            rr:child \"" + predicate_object.object_map.child[0] + "\";\n"
                                                    mapping += "            rr:parent \"" + predicate_object.object_map.parent[0] + "\";\n"
                                                    mapping += "            ]\n"
                                                    mapping += "            ]\n"
                                                else:
                                                    value = proyections.pop(triples_map.data_source, None)
                                                    mapping += "            rr:template \"" + tm.subject_map.value + "\"\n"
                                                    object_attr = extract_attr(tm.subject_map.value,[])
                                                    join_data_sources(predicate_object.object_map.child[0], predicate_object.object_map.parent[0], 
                                                                    triples_map.data_source, tm.data_source, 
                                                                    subject_attr, object_attr, source)
                                                    mapping += "        ]\n"    
                                            mapping += "    ];\n"

        mapping = mapping[:-2]
        mapping += ".\n\n"

    i = 0
    for tm_id in non_func:
        for tm in triples_map_list:
            if tm_id == tm.triples_map_id:
                for predicate_object in tm.predicate_object_maps_list:
                    if predicate_object.predicate_map.value in non_func[tm_id]:
                        mapping += "<" + tm.triples_map_id + "_non_func_" + str(i) + ">\n"
                        mapping += "    a rr:TriplesMap;\n"
                        if str(tm.file_format).lower() == "csv" and tm.query == "None":
                            source = output_folder + "/" + tm.data_source.split("/")[len(tm.data_source.split("/"))-1].split(".")[0] + "_non_func_" + str(i) + ".csv"
                            mapping += "    rml:logicalSource [ rml:source \"" + source +"\";\n"
                            mapping += "                rml:referenceFormulation ql:CSV\n"
                            if tm.data_source in proyections:
                                proyections[tm.data_source][source] = []
                            else:
                                proyections[tm.data_source] = {source:[]}
                        mapping += "                ];\n"

                        mapping += "    rr:subjectMap [\n"
                        if "template" in tm.subject_map.subject_mapping_type:
                            proyections[tm.data_source][source] = extract_attr(tm.subject_map.value, proyections[tm.data_source][source])
                            mapping += "        rr:template \"" + tm.subject_map.value + "\";\n"
                        elif "reference" in tm.subject_map.subject_mapping_type:
                            if tm.subject_map.value not in proyections[tm.data_source][source]:
                                proyections[tm.data_source][source].append(tm.subject_map.value)
                            mapping += "        rml:reference \"" + triples_map.subject_map.value + "\";\n"
                            mapping += "        rr:termType rr:IRI\n"
                        elif "constant" in tm.subject_map.subject_mapping_type:
                            mapping += "        rr:constant \"" + tm.subject_map.value + "\";\n"
                            mapping += "        rr:termType rr:IRI\n"
                        if triples_map.subject_map.rdf_class[0] != None:
                            prefix, url, value = prefix_extraction(original, tm.subject_map.rdf_class[0])
                            mapping += "        rr:class " + prefix + ":" + value  + "\n"
                        mapping += "    ];\n"
                        mapping += "    rr:predicateObjectMap [\n"
                        if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                            prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                            mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                        elif "constant" in predicate_object.predicate_map.mapping_type :
                            prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                            if prefix != None:
                                mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                            else:
                                mapping += "        rr:predicateMap[\n"
                                mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                mapping += "        ];\n"
                        elif "template" in predicate_object.predicate_map.mapping_type:
                            mapping += "        rr:predicateMap[\n"
                            mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                            mapping += "        ];\n"
                        elif "reference" in predicate_object.predicate_map.mapping_type:
                            mapping += "        rr:predicateMap[\n"
                            mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                            mapping += "        ];\n"

                        mapping += "        rr:objectMap "
                        if "constant" in predicate_object.object_map.mapping_type:
                            mapping += "[\n"
                            mapping += "            rr:constant \"" + predicate_object.object_map.value + "\"\n"
                            mapping += "        ]\n"
                        elif "template" in predicate_object.object_map.mapping_type:
                            proyections[tm.data_source][source] = extract_attr(predicate_object.object_map.value,proyections[tm.data_source][source])
                            mapping += "[\n"
                            mapping += "            rr:template  \"" + predicate_object.object_map.value + "\"\n"
                            mapping += "        ]\n"
                        elif "reference" == predicate_object.object_map.mapping_type:
                            if predicate_object.object_map.value not in proyections[tm.data_source][source]:
                                proyections[tm.data_source][source].append(predicate_object.object_map.value)
                            mapping += "[\n"
                            mapping += "            rml:reference \"" + predicate_object.object_map.value + "\"\n"
                            if predicate_object.object_map.datatype is not None:
                                prefix, url, value = prefix_extraction(original, predicate_object.object_map.datatype)
                                mapping = mapping[:-1]
                                mapping += ";\n            rr:datatype " + prefix + ":" + value + ";\n"
                            elif predicate_object.object_map.term is not None:
                                prefix, url, value = prefix_extraction(original, predicate_object.object_map.term)
                                mapping = mapping[:-1]
                                mapping += ";\n            rr:termType " + prefix + ":" + value + ";\n"
                            mapping += "        ]\n"
                        elif "constant shortcut" in predicate_object.object_map.mapping_type:
                            mapping += "[\n"
                            mapping += "            rr:constant \"" + predicate_object.object_map.value + "\"\n"
                            mapping += "        ]\n"
                        elif "parent triples map" not in predicate_object.object_map.mapping_type:
                            for tm_element in triples_map_list:
                                if tm_element.triples_map_id == predicate_object.object_map.value:
                                    if tm_element.data_source == tm.data_source:
                                        if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                                            if predicate_object.object_map.child == predicate_object.object_map.parent:
                                                proyections[tm.data_source][source] = extract_attr(tm_element.subject_map.value,proyections[tm.data_source][source])
                                                mapping += "[\n"
                                                mapping += "            rr:template  \"" + tm_element.subject_map.value  + "\"\n"
                                                mapping += "        ]\n"
                                        else:
                                            proyections[tm.data_source][source] = extract_attr(tm_element.subject_map.value,proyections[tm.data_source][source])
                                            mapping += " [\n"
                                            mapping += "            rr:template  \"" + tm_element.subject_map.value  + "\"\n"
                                            mapping += "        ]\n"
                        mapping += "    ].\n\n"
                        i += 1


    i = 0
    for tm_id in child_triples_maps:
        for predicate_object in child_triples_maps[tm_id].predicate_object_maps_list:
            if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                mapping += "<" + child_triples_maps[tm_id].triples_map_id + "_child_" + str(i) + ">\n"
                mapping += "    a rr:TriplesMap;\n"
                if str(child_triples_maps[tm_id].file_format).lower() == "csv" and child_triples_maps[tm_id].query == "None":
                    source = output_folder + "/" + child_triples_maps[tm_id].data_source.split(".")[0].split("/")[len(tm.data_source.split(".")[0].split("/"))-1]  + "_child_" + str(i) + ".csv"
                    if child_triples_maps[tm_id].data_source in proyections:
                        proyections[child_triples_maps[tm_id].data_source][source] = []
                    else:
                        proyections[child_triples_maps[tm_id].data_source] = {source:[]}
                    mapping += "    rml:logicalSource [ rml:source \"" + source +"\";\n"
                    mapping += "                rml:referenceFormulation ql:CSV\n"
                mapping += "                ];\n"
                mapping += "    rr:subjectMap [\n"
                if "template" in child_triples_maps[tm_id].subject_map.subject_mapping_type:
                    mapping += "        rr:template \"" + child_triples_maps[tm_id].subject_map.value + "\";\n"
                    proyections[child_triples_maps[tm_id].data_source][source] = extract_attr(child_triples_maps[tm_id].subject_map.value,proyections[child_triples_maps[tm_id].data_source][source])
                elif "reference" in child_triples_maps[tm_id].subject_map.subject_mapping_type:
                    if child_triples_maps[tm_id].subject_map.value not in proyections[child_triples_maps[tm_id].data_source][source]:
                        proyections[child_triples_maps[tm_id].data_source][source].append(child_triples_maps[tm_id].subject_map.value)
                    mapping += "        rml:reference \"" + child_triples_maps[tm_id].subject_map.value + "\";\n"
                    mapping += "        rr:termType rr:IRI\n"
                elif "constant" in child_triples_maps[tm_id].subject_map.subject_mapping_type:
                    mapping += "        rr:constant \"" + child_triples_maps[tm_id].subject_map.value + "\";\n"
                    mapping += "        rr:termType rr:IRI\n"
                if child_triples_maps[tm_id].subject_map.rdf_class[0] != None:
                    prefix, url, value = prefix_extraction(original, triples_map.subject_map.rdf_class[0])
                    mapping += "        rr:class " + prefix + ":" + value  + "\n"
                mapping += "    ];\n"
                mapping += "    rr:predicateObjectMap [\n"
                if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                    prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                    mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                elif "constant" in predicate_object.predicate_map.mapping_type :
                    prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                    if prefix != None:
                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                    else:
                        mapping += "        rr:predicateMap[\n"
                        mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                        mapping += "        ];\n"
                elif "template" in predicate_object.predicate_map.mapping_type:
                    mapping += "        rr:predicateMap[\n"
                    mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                    mapping += "        ];\n"
                elif "reference" in predicate_object.predicate_map.mapping_type:
                    mapping += "        rr:predicateMap[\n"
                    mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                    mapping += "        ];\n"
                mapping += "        rr:objectMap [\n"
                for tm in triples_map_list:
                    if predicate_object.object_map.value == tm.triples_map_id:
                        if type_join(tm.data_source,triples_map.data_source, predicate_object.object_map.parent[0],predicate_object.object_map.child[0]):
                            if predicate_object.object_map.value in parent_triples_maps:
                                parent = str(len(parent_triples_maps[predicate_object.object_map.value])+1)
                                parent_triples_maps[predicate_object.object_map.value][predicate_object.object_map.value+"_parent_"+parent] = predicate_object.object_map.parent[0]
                                mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+"_parent_" + parent + ">;\n"
                            else:
                                parent_triples_maps[predicate_object.object_map.value] = {predicate_object.object_map.value+"_parent_1":predicate_object.object_map.parent[0]}
                                mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+"_parent_1" + ">;\n"
                            mapping += "        rr:joinCondition [\n"
                            mapping += "            rr:child \"" + predicate_object.object_map.child[0] + "\";\n"
                            mapping += "            rr:parent \"" + predicate_object.object_map.parent[0] + "\";\n"
                            mapping += "            ]\n"
                            if predicate_object.object_map.child[0] not in proyections[child_triples_maps[tm_id].data_source][source]:
                                proyections[child_triples_maps[tm_id].data_source][source].append(predicate_object.object_map.child[0])
                        else:
                            value = proyections[child_triples_maps[tm_id].data_source].pop(source, None)
                            mapping += "            rr:template \"" + tm.subject_map.value + "\"\n"
                            subject_attr = extract_attr(child_triples_maps[tm_id].subject_map.value,[])
                            object_attr = extract_attr(tm.subject_map.value,[])
                            join_data_sources(predicate_object.object_map.child[0], predicate_object.object_map.parent[0], 
                                            child_triples_maps[tm_id].data_source, tm.data_source, 
                                            subject_attr, object_attr, source)
                mapping += "        ]\n"
                mapping += "    ].\n\n"
                
                i += 1


    for tm in triples_map_list:
        if tm.triples_map_id in parent_triples_maps:
            for parent in parent_triples_maps[tm.triples_map_id]:
                attr = []
                if tm.data_source in proyections:
                    source = output_folder + "/" + tm.data_source.split(".")[0].split("/")[len(tm.data_source.split(".")[0].split("/"))-1] + "_parent_" + str(len(proyections[tm.data_source])+1) + ".csv"
                else:
                    source = output_folder + "/" + tm.data_source.split(".")[0].split("/")[len(tm.data_source.split(".")[0].split("/"))-1] + "_parent_1.csv"
                mapping += "<" + parent + ">\n"
                mapping += "    a rr:TriplesMap;\n"
                mapping += "    rml:logicalSource [ rml:source \"" + source +"\";\n"
                mapping += "                rml:referenceFormulation ql:CSV\n"
                mapping += "            ];\n"
                mapping += "    rr:subjectMap [\n"
                mapping += "        rr:template \"" + tm.subject_map.value + "\";\n"
                attr = extract_attr(tm.subject_map.value,attr)
                if parent_triples_maps[tm.triples_map_id][parent] not in attr:
                    attr.append(parent_triples_maps[tm.triples_map_id][parent])
                if triples_map.subject_map.rdf_class[0] != None:
                        prefix, url, value = prefix_extraction(original, tm.subject_map.rdf_class[0])
                        mapping += "        rr:class " + prefix + ":" + value  + "\n"
                mapping += "    ].\n\n"
                if tm.data_source in proyections:
                    proyections[tm.data_source][source] = attr
                else:
                    proyections[tm.data_source] = {source:attr}

    prefix_string = ""
    f = open(original,"r")
    original_mapping = f.readlines()
    for prefix in original_mapping:
        if "prefix;" in prefix or "d2rq:Database;" in prefix:
            pass
        elif ("prefix" in prefix) or ("base" in prefix):
           prefix_string += prefix
    f.close()

    prefix_string += "\n"
    prefix_string += mapping
    mapping_file = open(output_folder + "/" + original.split("/")[len(original.split("/"))-1].split(".")[0] + "_func.ttl","w")
    mapping_file.write(prefix_string)
    mapping_file.close()

    for file in proyections:
        for pf in proyections[file]:
            reader = pd.read_csv(file,usecols=proyections[file][pf], low_memory=False)
            reader = reader.where(pd.notnull(reader), None)
            reader = reader.drop_duplicates(keep ='first')
            data = reader.to_dict(orient='records')
            with open(pf, 'w', newline='') as proyected_file:
                writer = csv.writer(proyected_file)
                writer.writerow(proyections[file][pf])
                for row in data:
                    new_row = []
                    for attr in proyections[file][pf]:
                        new_row.append(row[attr])
                    writer.writerow(new_row)

    if engine != "":
        execute_engines(engine, original.split("/")[len(original.split("/"))-1].split(".")[0] + "_func.ttl", output_folder, output_name)

def simple_projection(original, output_folder, triples_map_list):
    mapping = ""
    parent_triples_maps = {}
    child_triples_maps = {}
    proyections = {}

    for triples_map in triples_map_list:
        if len(triples_map.predicate_object_maps_list) > 0 and triples_map.predicate_object_maps_list[0].predicate_map.value != "None":
            if triples_map.data_source in proyections:
                source = output_folder + "/" + triples_map.data_source.split(".")[0].split("/")[len(triples_map.data_source.split(".")[0].split("/"))-1] + "_proyected_"+ str(len(proyections[triples_map.data_source])+1) +".csv"
                proyections[triples_map.data_source][source] = []
            else:
                source = output_folder + "/" + triples_map.data_source.split(".")[0].split("/")[len(triples_map.data_source.split(".")[0].split("/"))-1] + "_proyected_1.csv"
                proyections[triples_map.data_source] = {source:[]}
        else:
            source = triples_map.data_source
        mapping += "<" + triples_map.triples_map_id + ">\n"
        mapping += "    a rr:TriplesMap;\n"
        if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None":
            mapping += "    rml:logicalSource [ rml:source \"" + source +"\";\n"
            mapping += "                rml:referenceFormulation ql:CSV\n"
        mapping += "                ];\n"

        mapping += "    rr:subjectMap [\n"
        if "template" in triples_map.subject_map.subject_mapping_type:
            if len(triples_map.predicate_object_maps_list) > 0 and triples_map.predicate_object_maps_list[0].predicate_map.value != "None":
                proyections[triples_map.data_source][source] = extract_attr(triples_map.subject_map.value, proyections[triples_map.data_source][source])
            mapping += "        rr:template \"" + triples_map.subject_map.value + "\";\n"
        elif "reference" in triples_map.subject_map.subject_mapping_type:
            if len(triples_map.predicate_object_maps_list) > 0 and triples_map.predicate_object_maps_list[0].predicate_map.value != "None":
                if triples_map.subject_map.value not in proyections[triples_map.data_source][source]:
                    proyections[triples_map.data_source][source].append(triples_map.subject_map.value)
            mapping += "        rml:reference \"" + triples_map.subject_map.value + "\";\n"
            mapping += "        rr:termType rr:IRI\n"
        elif "constant" in triples_map.subject_map.subject_mapping_type:
            mapping += "        rr:constant \"" + triples_map.subject_map.value + "\";\n"
            mapping += "        rr:termType rr:IRI\n"
        if triples_map.subject_map.rdf_class[0] != None:
            prefix, url, value = prefix_extraction(original, triples_map.subject_map.rdf_class[0])
            mapping += "        rr:class " + prefix + ":" + value  + "\n"
        mapping += "    ];\n"

        if len(triples_map.predicate_object_maps_list) > 0 and triples_map.predicate_object_maps_list[0].predicate_map.value != "None":
            for predicate_object in triples_map.predicate_object_maps_list:
                if "parent triples map" not in predicate_object.object_map.mapping_type:
                    mapping += "    rr:predicateObjectMap [\n"
                    if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                    elif "constant" in predicate_object.predicate_map.mapping_type :
                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                        if prefix != None:
                            mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                        else:
                            mapping += "        rr:predicateMap[\n"
                            mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                            mapping += "        ];\n"
                    elif "template" in predicate_object.predicate_map.mapping_type:
                        mapping += "        rr:predicateMap[\n"
                        mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                        mapping += "        ];\n"
                    elif "reference" in predicate_object.predicate_map.mapping_type:
                        mapping += "        rr:predicateMap[\n"
                        mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                        mapping += "        ];\n"

                    mapping += "        rr:objectMap "
                    if "constant" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "            rr:constant \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "template" in predicate_object.object_map.mapping_type:
                        proyections[triples_map.data_source][source] = extract_attr(predicate_object.object_map.value,proyections[triples_map.data_source][source])
                        mapping += "[\n"
                        mapping += "            rr:template  \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "reference" == predicate_object.object_map.mapping_type:
                        if predicate_object.object_map.value not in proyections[triples_map.data_source][source]:
                            proyections[triples_map.data_source][source].append(predicate_object.object_map.value)
                        mapping += "[\n"
                        mapping += "            rml:reference \"" + predicate_object.object_map.value + "\"\n"
                        if predicate_object.object_map.datatype is not None:
                            prefix, url, value = prefix_extraction(original, predicate_object.object_map.datatype)
                            mapping = mapping[:-1]
                            mapping += ";\n            rr:datatype " + prefix + ":" + value + ";\n"
                        elif predicate_object.object_map.term is not None:
                            prefix, url, value = prefix_extraction(original, predicate_object.object_map.term)
                            mapping = mapping[:-1]
                            mapping += ";\n            rr:termType " + prefix + ":" + value + ";\n"
                        mapping += "        ]\n"
                    elif "constant shortcut" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "            rr:constant \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    mapping += "    ];\n"
                else:
                    for tm in triples_map_list:
                        if tm.triples_map_id == predicate_object.object_map.value:
                            if tm.data_source == triples_map.data_source:
                                if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                                    """if predicate_object.object_map.child == predicate_object.object_map.parent:
                                        mapping += "    rr:predicateObjectMap [\n"
                                        if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                                            prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                            mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                        elif "constant" in predicate_object.predicate_map.mapping_type :
                                            prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                            if prefix != None:
                                                mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                            else:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                        elif "template" in predicate_object.predicate_map.mapping_type:
                                            mapping += "        rr:predicateMap[\n"
                                            mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                                            mapping += "        ];\n"
                                        elif "reference" in predicate_object.predicate_map.mapping_type:
                                            mapping += "        rr:predicateMap[\n"
                                            mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                                            mapping += "        ];\n"
                                        proyections[triples_map.data_source][source] = extract_attr(tm.subject_map.value,proyections[triples_map.data_source][source])
                                        mapping += "        rr:objectMap [\n"
                                        mapping += "            rr:template  \"" + tm.subject_map.value  + "\"\n"
                                        mapping += "        ]\n"
                                        mapping += "    ];\n"
                                    else:"""
                                    if len(triples_map.predicate_object_maps_list) > 1:
                                        if triples_map.triples_map_id not in child_triples_maps:
                                            child_triples_maps[triples_map.triples_map_id] = triples_map
                                    else:
                                        if tm.triples_map_id == predicate_object.object_map.value:
                                            mapping += "    rr:predicateObjectMap [\n"
                                            if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                                                prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                            elif "constant" in predicate_object.predicate_map.mapping_type :
                                                prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                if prefix != None:
                                                    mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                                else:
                                                    mapping += "        rr:predicateMap[\n"
                                                    mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                                    mapping += "        ];\n"
                                            elif "template" in predicate_object.predicate_map.mapping_type:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                            elif "reference" in predicate_object.predicate_map.mapping_type:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                            if predicate_object.object_map.child[0] not in proyections[triples_map.data_source][source]:
                                                proyections[triples_map.data_source][source].append(predicate_object.object_map.child[0])
                                            mapping += "        rr:objectMap [\n"
                                            if predicate_object.object_map.value in parent_triples_maps:
                                                parent = str(len(parent_triples_maps[predicate_object.object_map.value])+1)
                                                parent_triples_maps[predicate_object.object_map.value][predicate_object.object_map.value+"_parent_"+parent] = predicate_object.object_map.parent[0]
                                                mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+"_parent_" + parent + ">;\n"
                                            else:
                                                parent_triples_maps[predicate_object.object_map.value] = {predicate_object.object_map.value+"_parent_1":predicate_object.object_map.parent[0]}
                                                mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+"_parent_1" + ">;\n"
                                            mapping += "        rr:joinCondition [\n"
                                            mapping += "            rr:child \"" + predicate_object.object_map.child[0] + "\";\n"
                                            mapping += "            rr:parent \"" + predicate_object.object_map.parent[0] + "\";\n"
                                            mapping += "            ]\n"
                                            mapping += "            ]\n"
                                            mapping += "        ];\n"

                                else:
                                    mapping += "    rr:predicateObjectMap [\n"
                                    if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                    elif "constant" in predicate_object.predicate_map.mapping_type :
                                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                        if prefix != None:
                                            mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                        else:
                                            mapping += "        rr:predicateMap[\n"
                                            mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                            mapping += "        ];\n"
                                    elif "template" in predicate_object.predicate_map.mapping_type:
                                        mapping += "        rr:predicateMap[\n"
                                        mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                                        mapping += "        ];\n"
                                    elif "reference" in predicate_object.predicate_map.mapping_type:
                                        mapping += "        rr:predicateMap[\n"
                                        mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                                        mapping += "        ];\n"
                                    proyections[triples_map.data_source][source] = extract_attr(tm.subject_map.value,proyections[triples_map.data_source][source])
                                    mapping += "        rr:objectMap [\n"
                                    mapping += "            rr:template  \"" + tm.subject_map.value  + "\"\n"
                                    mapping += "        ]\n"
                                    mapping += "    ];\n"
                            else:
                                if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                                    if len(triples_map.predicate_object_maps_list) > 1:
                                        if triples_map.triples_map_id not in child_triples_maps:
                                            child_triples_maps[triples_map.triples_map_id] = triples_map
                                    else:
                                        if tm.triples_map_id == predicate_object.object_map.value:
                                            mapping += "    rr:predicateObjectMap [\n"
                                            if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                                                prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                            elif "constant" in predicate_object.predicate_map.mapping_type :
                                                prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                                if prefix != None:
                                                    mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                                else:
                                                    mapping += "        rr:predicateMap[\n"
                                                    mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                                                    mapping += "        ];\n"
                                            elif "template" in predicate_object.predicate_map.mapping_type:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                            elif "reference" in predicate_object.predicate_map.mapping_type:
                                                mapping += "        rr:predicateMap[\n"
                                                mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                                                mapping += "        ];\n"
                                            mapping += "        rr:objectMap [\n"
                                            if predicate_object.object_map.child[0] not in proyections[triples_map.data_source][source]:
                                                proyections[triples_map.data_source][source].append(predicate_object.object_map.child[0])
                                            if predicate_object.object_map.value in parent_triples_maps:
                                                parent = str(len(parent_triples_maps[predicate_object.object_map.value])+1)
                                                parent_triples_maps[predicate_object.object_map.value][predicate_object.object_map.value+"_parent_"+parent] = predicate_object.object_map.parent[0]
                                                mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+"_parent_" + parent + ">;\n"
                                            else:
                                                parent_triples_maps[predicate_object.object_map.value] = {predicate_object.object_map.value+"_parent_1":predicate_object.object_map.parent[0]}
                                                mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value +"_parent_1" + ">;\n"
                                            mapping += "        rr:joinCondition [\n"
                                            mapping += "            rr:child \"" + predicate_object.object_map.child[0] + "\";\n"
                                            mapping += "            rr:parent \"" + predicate_object.object_map.parent[0] + "\";\n"
                                            mapping += "            ]\n"
                                            mapping += "            ]\n"
                                            mapping += "        ];\n"

        mapping = mapping[:-2]
        mapping += ".\n\n"

    i = 0
    for tm_id in child_triples_maps:
        for predicate_object in child_triples_maps[tm_id].predicate_object_maps_list:
            if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                mapping += "<" + child_triples_maps[tm_id].triples_map_id + "_child_" + str(i) + ">\n"
                mapping += "    a rr:TriplesMap;\n"
                if str(child_triples_maps[tm_id].file_format).lower() == "csv" and child_triples_maps[tm_id].query == "None":
                    source = output_folder + "/" + child_triples_maps[tm_id].data_source.split(".")[0].split("/")[len(tm.data_source.split(".")[0].split("/"))-1] + "_child_" + str(i) + ".csv"
                    if child_triples_maps[tm_id].data_source in proyections:
                        proyections[child_triples_maps[tm_id].data_source][source] = []
                    else:
                        proyections[child_triples_maps[tm_id].data_source] = {source:[]}
                    mapping += "    rml:logicalSource [ rml:source \"" + source +"\";\n"
                    mapping += "                rml:referenceFormulation ql:CSV\n"
                mapping += "                ];\n"
                mapping += "    rr:subjectMap [\n"
                if "template" in child_triples_maps[tm_id].subject_map.subject_mapping_type:
                    mapping += "        rr:template \"" + child_triples_maps[tm_id].subject_map.value + "\";\n"
                    proyections[child_triples_maps[tm_id].data_source][source] = extract_attr(child_triples_maps[tm_id].subject_map.value,proyections[child_triples_maps[tm_id].data_source][source])
                elif "reference" in child_triples_maps[tm_id].subject_map.subject_mapping_type:
                    if child_triples_maps[tm_id].subject_map.value not in proyections[child_triples_maps[tm_id].data_source][source]:
                        proyections[child_triples_maps[tm_id].data_source][source].append(child_triples_maps[tm_id].subject_map.value)
                    mapping += "        rml:reference \"" + child_triples_maps[tm_id].subject_map.value + "\";\n"
                    mapping += "        rr:termType rr:IRI\n"
                elif "constant" in child_triples_maps[tm_id].subject_map.subject_mapping_type:
                    mapping += "        rr:constant \"" + child_triples_maps[tm_id].subject_map.value + "\";\n"
                    mapping += "        rr:termType rr:IRI\n"
                if child_triples_maps[tm_id].subject_map.rdf_class[0] != None:
                    prefix, url, value = prefix_extraction(original, triples_map.subject_map.rdf_class[0])
                    mapping += "        rr:class " + prefix + ":" + value  + "\n"
                mapping += "    ];\n"
                mapping += "    rr:predicateObjectMap [\n"
                if "constant shortcut" in predicate_object.predicate_map.mapping_type:
                    prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                    mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                elif "constant" in predicate_object.predicate_map.mapping_type :
                    prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                    if prefix != None:
                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                    else:
                        mapping += "        rr:predicateMap[\n"
                        mapping += "            rr:constant \"" + predicate_object.predicate_map.value + "\"\n"
                        mapping += "        ];\n"
                elif "template" in predicate_object.predicate_map.mapping_type:
                    mapping += "        rr:predicateMap[\n"
                    mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                    mapping += "        ];\n"
                elif "reference" in predicate_object.predicate_map.mapping_type:
                    mapping += "        rr:predicateMap[\n"
                    mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                    mapping += "        ];\n"
                mapping += "rr:objectMap [\n"
                if predicate_object.object_map.value in parent_triples_maps:
                    parent = str(len(parent_triples_maps[predicate_object.object_map.value])+1)
                    parent_triples_maps[predicate_object.object_map.value][predicate_object.object_map.value+"_parent_"+ parent] = predicate_object.object_map.parent[0]
                    mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+"_parent_" +parent + ">;\n"
                else:
                    parent_triples_maps[predicate_object.object_map.value] = {predicate_object.object_map.value+"_parent_1":predicate_object.object_map.parent[0]}
                    mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value+"_parent_1" + ">;\n"
                mapping += "        rr:joinCondition [\n"
                mapping += "            rr:child \"" + predicate_object.object_map.child[0] + "\";\n"
                mapping += "            rr:parent \"" + predicate_object.object_map.parent[0] + "\";\n"
                mapping += "            ]\n"
                mapping += "        ]\n"
                mapping += "].\n\n"
                if predicate_object.object_map.child[0] not in proyections[child_triples_maps[tm_id].data_source][source]:
                    proyections[child_triples_maps[tm_id].data_source][source].append(predicate_object.object_map.child[0])
                i += 1


    for tm in triples_map_list:
        if tm.triples_map_id in parent_triples_maps:
            for parent in parent_triples_maps[tm.triples_map_id]:
                attr = []
                if tm.data_source in proyections:
                    source = output_folder + "/" + tm.data_source.split(".")[0].split("/")[len(tm.data_source.split(".")[0].split("/"))-1] + "_parent_" + str(len(proyections[tm.data_source])+1) + ".csv"
                else:
                    source = output_folder + "/" + tm.data_source.split(".")[0].split("/")[len(tm.data_source.split(".")[0].split("/"))-1] + "_parent_1.csv"
                mapping += "<" + parent + ">\n"
                mapping += "    a rr:TriplesMap;\n"
                mapping += "    rml:logicalSource [ rml:source \"" + source +"\";\n"
                mapping += "                rml:referenceFormulation ql:CSV\n"
                mapping += "            ];\n"
                mapping += "    rr:subjectMap [\n"
                mapping += "        rr:template \"" + tm.subject_map.value + "\";\n"
                attr = extract_attr(tm.subject_map.value,attr)
                if parent_triples_maps[tm.triples_map_id][parent] not in attr:
                    attr.append(parent_triples_maps[tm.triples_map_id][parent])
                if triples_map.subject_map.rdf_class[0] != None:
                        prefix, url, value = prefix_extraction(original, tm.subject_map.rdf_class[0])
                        mapping += "        rr:class " + prefix + ":" + value  + "\n"
                mapping += "    ].\n\n"
                if tm.data_source in proyections:
                    proyections[tm.data_source][source] = attr
                else:
                    proyections[tm.data_source] = {source:attr}

    prefix_string = ""
    f = open(original,"r")
    original_mapping = f.readlines()
    for prefix in original_mapping:
        if "prefix;" in prefix or "d2rq:Database;" in prefix:
            pass
        elif ("prefix" in prefix) or ("base" in prefix):
           prefix_string += prefix
    f.close()

    prefix_string += "\n"
    prefix_string += mapping
    mapping_file = open(output_folder + "/" + original.split("/")[len(original.split("/"))-1].split(".")[0] + "_simple.ttl","w")
    mapping_file.write(prefix_string)
    mapping_file.close()

    for file in proyections:
        for pf in proyections[file]:
            reader = pd.read_csv(file,usecols=proyections[file][pf], low_memory=False)
            reader = reader.where(pd.notnull(reader), None)
            reader = reader.drop_duplicates(keep ='first')
            data = reader.to_dict(orient='records')
            with open(pf, 'w', newline='') as proyected_file:
                writer = csv.writer(proyected_file)
                writer.writerow(proyections[file][pf])
                for row in data:
                    new_row = []
                    for attr in proyections[file][pf]:
                        new_row.append(row[attr])
                    writer.writerow(new_row)