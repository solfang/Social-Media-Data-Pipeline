import cv2
from tqdm import tqdm
import os
import numpy as np
from .anonymization.anonymize_face import anonymize_image_from_path


class ImageAnonymizer:
    """
    Pixelates faces of people in an image folder using a deep neural network.
    Uses the functionality and net provided by IDP 1 (Lukas Vordemann)
    """

    def __init__(self, image_folder, output_folder, confidence=0.2, in_place=False, skip_if_exists=False):
        """
        :param image_folder: input folder
        :param output_folder: pixelated images will be stored here. Not used if in_place=True.
        :param confidence: 0-1 value indicating how easily the face recognition tries to find faces 0=very aggresive, 1=not
        :param in_place: if true, replace the input images with the pixelated versions
        :param skip_if_exists: skip if the input images have already been pixelated
        """
        self.image_folder = image_folder
        self.output_folder = output_folder
        self.confidence = confidence
        self.in_place = in_place
        self.skip_if_exists = skip_if_exists

    def run(self):
        if self.in_place:
            # extraordinary lazy way to check that an image folder has been anonymized in-place
            # ('_anonymized' folder acts as marker and will be created after anonymization)
            anon_folder = os.path.join(os.path.dirname(self.image_folder), "_anonymized")
            if os.path.exists(anon_folder) and self.skip_if_exists:
                print("Images are already anonymized. Skipping.")
                return
        else:
            os.makedirs(self.output_folder, exist_ok=True)
        images = os.listdir(self.image_folder)

        for img in tqdm(images):
            img_path = os.path.join(self.image_folder, img)
            if self.skip_if_exists and os.path.exists(img_path) and not self.in_place:
                continue
            frame = anonymize_image_from_path(img_path, preset_confidence=self.confidence)
            if self.in_place:
                cv2.imwrite(img_path, frame)
            else:
                cv2.imwrite(os.path.join(self.output_folder, img), frame)

        if self.in_place:
            os.makedirs(anon_folder)


if __name__ == "__main__":
    anonymizer = ImageAnonymizer("test_images", "output")
    anonymizer.run()
