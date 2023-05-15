# Scraping and Preprocessing pipeline

## How to run
1. Get an API key from https://rapidapi.com/logicbuilder/api/instagram-data1 (paid service) and create a file under Scraper/RapidAPI/api_key.py with `API_KEY="..."` (+make sure the file is in gitignore)
2. run `orchestrator.py`. The default config downloads a dummy dataset from Instagram and runs the pipeline on it.
3. You may need to do additional steup to run the full pipeline, see *Setup* section.

 Optionally specify the arguments
  - `--config`: path to a pipeline config file (default: config/test.json - this will download a dummy dataset)
  - `--root_dir`: path to a root directory where the pipeline output will be stored (default: ../data/social_media_scraping)
   
   
## The Pipeline
The pipeline contains multiple processing stages and is defined by a config file, see config/ folder for examples.
`orchestrator.py` manages the pipeline execution by reading a config file and executing the stages.

## The config file
The config file defines some basic info a well as a set of stages to be executed.
- `dataset_name` (string): the pipeline output will be stored to and read from [dataset_name]/[root_dir] (root directory is given to orchestrator.py)
- `skip_stage_if_exists` (bool): if the output of a stage already exists it will be skipped
- `stages`: list of stages containing:
	- `name` (string): name of the stage (can be whatever)
	- `implementation` (string): Should correspond to one of the stages defined in `stages.py`
	- `input` (string or null): input file path for the stage w.r.t the root directory 
	- `output` (string): output file path for the stage w.r.t the root directory 
	- `enabled` (bool): whether the stage will be exectued
	- `params` (dict): stage-specific parameters to be passed to the stage -> see the stage classes for what these parameters do
 
## The stages

You can create your own stages via the config file. Though you will need to pass an implementation for that stage that the pipeline can execute.

Currently, these implementations are available:
| Stage                      | Function                                                                       |
|----------------------------|--------------------------------------------------------------------------------|
| **InstagramFeedScraperStage**  | Scrapes content from Instagram given a search term                             |
| **PreprocessorStage**          | Pre-processses posts by filtering etc.                                         |
| **ExploratoryanalysisStage**   | Does some shallow summary and basic plotting of important variables            |
| **TranslatorStage**            | Translates text                                                                |
| **InstagramImageScraperStage** | Scrapes the images associated with the posts                                   |
| **ImageLabelerStage**          | Labels images using the Places365-CNN                                          |
| **ImageFeatureVectorStage**    | Caluclates feature vectors using https://github.com/naver/deep-image-retrieval |
| **ImageAnonymizerStage**       | Pixelates faces in the images                                                  |

The stage implementations are defined in `stages.py`.
Requirements between stages, e.g. that stage x has to run before stage y is only defined implicitly since if a stage runs without the previous stage the input file may not be there from the previous stage.

The classes in `stages.py` parse the stage parameters and delegate the actual work. They're just there to provice a common interface.
The delegates, e.g. `Preprocessing/ImageLabeling/ImageLabeler.py` are fully functional by themselves outside of the pipeline if you prefer to use them that way.

## Setup
This depends on the stages you want to use. Specifc setup:
- InstagramFeedScraperStage: Purchase a subscription at https://rapidapi.com/logicbuilder/api/instagram-data1 and place the api-key at `Scraper/RapidAPI/api_key.py` as API_KEY="..."
- ImageLabelerStage: All the necessary files should come with this repo `Preprocessing/ImageLabeling`. You don't need to pull the Places365 repo separately.
- ImageFeatureVectorStage: 
	1. Pull the https://github.com/naver/deep-image-retrieval repo into `Preprocessing/FeatureVectors/deep-image-retrieval`
	2. Download the Resnet101-AP-GeM-LM18 from https://github.com/naver/deep-image-retrieval#pre-trained-models and place it at `.../deep-image-retrieval/dirtorch/models/Resnet101-AP-GeM-LM18.pt`
- ImageAnonymizerStage: requires a NN model, which comes with this repo

## Changing the pipeline

The current stages are tailored for Instagram data.

You can swap out specific stages, e.g. to scrape Crowdtangle instead of using a third-party Instagram scraper:
1. Create new stage classes in `scraper.py` that implement/delegate to the necessary functionality for Crowdtangle
2. Create a new config that declares these stages and change the parameters in the config to match the kind of data found in the Crowdtangle dataset.
Specifcally, you'd probably want to replace the post scraper, pre-processor and image scraper stages with Crowdtangle-specific solutions and change the params of the translation stage in the config. The rest of the stages should work as-is on the new data.