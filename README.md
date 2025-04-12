# NormaKG

NormaKG is a versatile tool that employs mapping partitioning and data normalization techniques, guided by functional dependencies, to enhance the performance of existing knowledge graph creation engines. As of now, NormaKG exclusively supports the projection of CSV files.

## Installing NormaKG
```
pip3 install -r requirements.txt
```

## Example of Configuration File

NormaKG offers two types of projections: Simple and Functional. The Simple projection (MapSDI) focuses on projecting only the necessary data sources and transforming Parent triples maps and self-joins into their equivalent `rr:template` operations. On the other hand, the Functional projection (NormaKG) is based on the functional dependencies of the data sources, ensuring that non-functional attributes are processed separately. This approach enables knowledge graph creation engines to run without the need to remove duplicates. Additionally, the Functional projection requires a file containing the functional dependencies of the data sources. Users when using the functional projection can generate the knowledge if provided with a knowledge graph creation engine. Currently, NormaKG can execute SDM-RDFizer, RMLMapper, Morph-KGC, RocketRML, and FlexRML. Please note, that corresponding libraries and executables for the engines must be downloaded separately. The `engine` parameter is the configuration file is optional. If not given, NormaKG will normalized the provided mappings and data sources. 

### Example of Simple Projection Configuration File

```
[default]
main_directory: .

[datasets]
number_of_datasets: 1
output_folder: ${default:main_directory}/simple

[dataset1]
name: test-case
mapping: ${default:main_directory}/mapping.ttl
```

### Example of Functional Projection Configuration File

```
[default]
main_directory: .

[datasets]
number_of_datasets: 1
output_folder: ${default:main_directory}/fd_proy
engine: SDM-RDFizer

[dataset1]
name: test-case
fd: ${default:main_directory}/gtfs_fd.ttl
mapping: ${default:main_directory}/simple/mapping_simple.ttl

```

## Generate Functional Dependencies File

The functional dependencies of a data source are determined by using [FDTool](https://github.com/USEPA/FDTool). Afterwards, the functional dependencies are organized in the following format:

```
@prefix fd: <http://example-fd-set.com/> .	

<#GENOMIC> fd:key [fd:column_name "{Gene CDS length, ID_tumour}";
	fd:determine [fd:column_name "Gene name";
					fd:dependant "{Accession Number,MUTATION_ID}";
				];
	fd:determine [fd:column_name "Gene CDS length";
					fd:dependant "{Accession Number}";
				];
	fd:determine [fd:column_name "pFormat";
					fd:dependant "{MUTATION_ID}"
				];
	fd:determine [fd:column_name "HGNC ID";
					fd:dependant "{Accession Number,MUTATION_ID}"
				];
	fd:determine [fd:column_name "ID_tumour";
					fd:dependant "{ID_sample}"
				];
	fd:determine [fd:column_name "Primary site";
					fd:dependant "{ID_sample,ID_tumou}"
				];
	fd:determine [fd:column_name "Accession Number";
					fd:dependant "{MUTATION_ID}"
				]
	].
```

- `fd:key` indicates which attributes from the data source can be used for the subject of a triples map.
- `fd:determine` indicates the association between an attributes and its dependants.
- `fd:column_name` illustrates the attribute in question.
- `fd:dependant` lists all the attributes that are associated to the attribute in `fd:column_name`.

## Running KGSaw
```
python3 run_fragmentor.py /path/to/config/file
```