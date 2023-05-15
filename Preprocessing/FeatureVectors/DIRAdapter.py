import os
import sys

# not used but needs to be set for the DIR execution
os.environ['DB_ROOT'] = "dummy"

"""
Makes the DIRtorch model available from inside a python file: constructs the shell command for running the model and executes it.
This was built for usage in the Pipeline.
You could totally just manually modify the command and run it in a shell outside of the Pipeline.

SETUP:
1. Pull the https://github.com/naver/deep-image-retrieval repo into a folder called 'deep-image-retrieval' (if you change it to something else adapt the variable repo_folder)
 expected folder structure: deep-image-retrieval/dirtorch/...
2. Download the Resnet101-AP-GeM-LM18 from https://github.com/naver/deep-image-retrieval#pre-trained-models and place it at dirtorch/models/Resnet101-AP-GeM-LM18.pt
 Alternatively download a different model and change the 'checkpoint' variable below
"""


def get_features(image_folder, image_list_file, output_file, gpu_id, repo_folder="deep-image-retrieval", skip_if_exists=False):
    """
    Runs the DIRtorch model on an image folder
    :param image_folder: path to the folder - feature vectors will be calculated for all images inside the folder
    :param image_list_file: path to the file that lists all images in image_folder - if it doesn't exist it will be created automatically
    :param output_file: path to the .npy file where the feature vectors are output
    :param gpu_id: -1 = use CPU, 1 = first GPU, 2 = second GPU etc. You WANT to use a GPU, else the feature calculation is painfully slow.
    :param repo_folder: path to the DIRtorch repository
    :param skip_if_exists: skip execution if the output file already exists
    """

    if os.path.exists(output_file) and skip_if_exists:
        print("Output file already exists. Skipping. Output file at {}".format(output_file))
        return

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # create the image list file if it doesn't exist yet
    if not os.path.exists(image_list_file):
        with open(image_list_file, "w") as f:
            image_list = os.listdir(image_folder)
            f.write("\n".join(image_list))

    # transform the paths to be relative to the DIR directory and make sure there are to backwards slashes
    # absolute paths throw an error during DIR execution
    image_folder_rel = os.path.relpath(image_folder, start=repo_folder).replace(os.sep, '/')
    image_list_file_rel = os.path.relpath(image_list_file, start=repo_folder).replace(os.sep, '/')
    output_file_rel = os.path.relpath(output_file, start=repo_folder).replace(os.sep, '/')

    # ---------------------------# DIRtorch Execution
    # we run the deep-image-retrieval model from within the deep-image-retrieval folder as indicated by the repo
    old_cwd = os.getcwd()
    os.chdir(repo_folder)

    checkpoint = "dirtorch/models/Resnet101-AP-GeM-LM18.pt"

    # create the dataset representation
    """
    the string version of a command that will be evaluated internally by DIR,see https://github.com/naver/deep-image-retrieval#feature-extractor
    looks something like: --dataset 'ImageList("PATH_TO_TEXTFILE" [, "IMAGES_ROOT"])'
    where PATH_TO_TEXTFILE is a textfile that lists the image names and IMAGES_ROOT points to the image folder
    the outer quotation marks are to deal with any space inside the command
    quotation marks inside the command need to be escaped for the shell execution
    """
    dataset_str = r'"ImageList(\"{}\" , \"{}\")"'.format(image_list_file_rel, image_folder_rel)

    print(dataset_str)

    # construct the shell command
    command = "python -m dirtorch.extract_features --dataset {} --checkpoint {} --output  \"{}\" --whiten Landmarks_clean --whitenp 0.25 --gpu {}".format(
        dataset_str, checkpoint, output_file_rel, gpu_id)
    print(command)
    os.system(command)
    # restore the old working directory
    print("Output saved to {}".format(os.path.abspath(output_file)))
    os.chdir(old_cwd)
    # ---------------------------#


if __name__ == "__main__":
    casestudy = "Test_test"
    image_folder = "../../data/datasets/04_download_images/{}/images".format(casestudy)  # DO: SET
    stage = "06_calc_image_vectors"
    output_file = "../../data/datasets/{}/{}.npy".format(stage, casestudy)
    image_list_file = "../../data/datasets/04_download_images/{}/image_db.txt".format(casestudy)
    get_features(image_folder, image_list_file, output_file)
