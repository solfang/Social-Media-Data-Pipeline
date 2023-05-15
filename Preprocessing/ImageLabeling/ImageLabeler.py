# PlacesCNN for scene classification
#
# by Bolei Zhou
# Edited by Lukas for the Master Thesis

import torch
from torch.autograd import Variable as V
import torchvision.models as models
from torchvision import transforms as trn
from torch.nn import functional as F
import os
from PIL import Image
from tqdm import tqdm
import pandas as pd
import pathlib

"""
This code runs a pre-trained Places-365 CNN (https://github.com/CSAILVision/places365) on an image dataset and outputs a table with the predictions + further scene info for each image
All necessary files (except the images) should be stored in the same folder as this file
Info regarding the scene hierarchy:
- The model outputs scene category predictions (e.g. airfield), from a total of 365 categories
- Each category can be related to one or more 'level 2' scenes (16 in total) 
  -> see https://docs.google.com/spreadsheets/d/1H7ADoEIGgbF_eXh9kcJjCs5j_r3VJwke4nebhkdzksg/edit?usp=sharing
- Each category can also be related to one or more 'level 1' scenes (3 in total): indoor,  outdoor (natural), outdoor (man-made)
This code predicts the top scene category and matches it with the corresping level1/level2 scene(s)

What you need to run this (all files should already be included in the repository that this file comes in but just in case):
1. categories_places365.txt 
 -> download from  https://github.com/CSAILVision/places365 (you don't need to download the full repo to run this script)
2. resnet50_places365.pth.tar or some other model 
 -> can be downloaded via the links in https://github.com/CSAILVision/places365
3. The scene hierachy file -> download this table as csv https://docs.google.com/spreadsheets/d/1H7ADoEIGgbF_eXh9kcJjCs5j_r3VJwke4nebhkdzksg/edit?usp=sharing
 -> rename it places365_scene_hierachy.csv
4. An image folder 
 -> provided by you
 
Note: to just test the model you can use test_images
Running the model on a lot of images will take a while (I can do ~10 images per second on Nvidia 2060super). 
Note to self: Something else might be the bottleneck tho, like reading/resizing the image. Can maybe parallelize.
"""


class ImageLabeler:

    def __init__(self, input_folder, output_file, architecture='resnet50', print_only=False, skip_if_exists=False):
        """
        :param input_folder: image folder path
        :param output_file: output csv path
        :param architecture: "resnet50", "resnet18", depending on what you downloaded (see Places365 repo link above)
        :param print_only: don't create a table, only print the classification results
        :param skip_if_exists: skip labeling of the output file already exists
        """
        self.input_folder = input_folder
        self.output_file = output_file
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        self.architecture = architecture
        self.print_only = print_only
        self.skip_if_exists = skip_if_exists

    def run(self):
        """
        Labels each image in the image folder using places365-CNN.
        The predicted labels are saved in a csv file together with the scenes corresponding to the label (given the scene hierarchy)
        """

        if os.path.exists(self.output_file) and self.skip_if_exists:
            print("Output file already exists. Skipping. Output file at", self.output_file)
            return

        # SETUP
        # the architecture to use. Make sure to download the respective model
        arch = self.architecture
        # get path to this file's parent folder
        resource_folder = pathlib.Path(__file__).parent.resolve()
        file_name = os.path.join(resource_folder, 'categories_places365.txt')  # label file name
        scene_file = os.path.join(resource_folder, "places365_scene_hierachy.csv")
        model_file = os.path.join(resource_folder, '%s_places365.pth.tar' % arch)
        image_folder = self.input_folder
        output_file = self.output_file
        limit = 0  # can set to only process a maximum of x images for testing, since processing takes a while. Set to 0 if no limit
        print_only = self.print_only  #

        if not os.path.exists(image_folder):
            print("Image folder not found. Using 'test_images' folder")
            image_folder = "test_images"

        # load the pre-trained weights
        # model_file = '%s_places365.pth.tar' % arch
        # if not os.access(model_file, os.W_OK):
        #     weight_url = 'http://places2.csail.mit.edu/models_places365/' + model_file
        #     os.system('wget ' + weight_url)



        # load resnet model from torch library
        model = models.__dict__[arch](num_classes=365)
        checkpoint = torch.load(model_file, map_location=lambda storage, loc: storage)
        state_dict = {str.replace(k, 'module.', ''): v for k, v in checkpoint['state_dict'].items()}
        model.load_state_dict(state_dict)
        model.eval()

        # load the image transformer
        centre_crop = trn.Compose([
            trn.Resize((256, 256)),
            trn.CenterCrop(224),
            trn.ToTensor(),
            trn.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
        ])

        # load the class label
        if not os.access(file_name, os.W_OK):
            synset_url = 'https://raw.githubusercontent.com/csailvision/places365/master/categories_places365.txt'
            os.system('wget ' + synset_url)
        classes = list()
        with open(file_name) as class_file:
            for line in class_file:
                classes.append(line.strip().split(' ')[0][3:])
        classes = tuple(classes)

        # read scene hierarchy
        df_scene = pd.read_csv(scene_file, skiprows=[0])  # skip the header row (Level 1	    Level 2, INDOOR:    etc.)
        # labels in the sheet come as e.g. '/a/airfield'. We remove the ''s and /a/ for consistency with the labels output by the model
        # i.e. '/a/airfield' -> airfield
        df_scene["category"] = df_scene["category"].apply(lambda x: x[4:-1])

        rows = []

        # read image names
        images = os.listdir(image_folder)
        if limit > 0:
            images = images[:limit]

        for image in tqdm(images):
            # load the test image
            img_name = os.path.join(image_folder, image)
            # if not os.access(img_name, os.W_OK):
            #     img_url = 'http://places.csail.mit.edu/demo/' + img_name
            # os.system('wget ' + img_url)

            img = Image.open(img_name)
            try:
                input_img = V(centre_crop(img).unsqueeze(0))
            except RuntimeError as e:  # black and white images throw an error
                print(e)
                print(image)
                continue

            # forward pass
            logit = model.forward(input_img)
            h_x = F.softmax(logit, 1).data.squeeze()
            probs, idx = h_x.sort(0, True)

            # construct dataframe row with ["image", "predictions", "category"]
            row = [image]
            row.append(
                [[classes[idx[i]], float(probs[i])] for i in range(0, 5)])  # get top 5 predictions as 2D array of [[category, confidence], ...]
            row.append(classes[idx[0]])  # get top prediction separately for convenience

            rows.append(row)

            if print_only:
                print('{} prediction on {}'.format(arch, img_name))
                # output the prediction
                for i in range(0, 5):
                    print('{:.3f} -> {}'.format(probs[i], classes[idx[i]]))

        df = pd.DataFrame(data=rows, columns=["image", "predictions", "category"])

        df = pd.merge(df, df_scene, on="category")

        if not print_only:
            df.to_csv(output_file, index=False)
            print("Output table saved to {}".format(output_file))
