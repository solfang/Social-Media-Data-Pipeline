""""
Code by Lukas Vordemann
last Edited by Lukas Leger: changed path base from cwd to this file's folder to work regardless of the caller's working directory
"""

import base64

import numpy as np
import cv2
import os


PROTO_TXT_ARGUMENTS = "pyimagesearch,deploy.prototxt".split(",")
WEIGHTS_MODEL_ARGUMENTS = "pyimagesearch,res10_300x300_ssd_iter_140000.caffemodel".split(",")
PROTO_TXT = os.path.join(*PROTO_TXT_ARGUMENTS)
WEIGHTS_MODEL = os.path.join(*WEIGHTS_MODEL_ARGUMENTS)

def anonymize_image(image, preset_confidence, blocks=3):
    proto_txt = os.path.join(os.path.dirname(os.path.realpath(__file__)), PROTO_TXT)
    weights_model = os.path.join(os.path.dirname(os.path.realpath(__file__)), WEIGHTS_MODEL)
    net = cv2.dnn.readNet(proto_txt, weights_model)
    image_pixeled = image.copy()
    (h, w) = image_pixeled.shape[:2]

    # construct blob
    blob = cv2.dnn.blobFromImage(image_pixeled, 1.0, (300, 300), (104.0, 177.0, 123.0))

    net.setInput(blob)
    detections = net.forward()

    for i in range(0, detections.shape[2]):
        confidence = detections[0, 0, i, 2]
        if confidence > float(preset_confidence):
            box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
            (startX, startY, endX, endY) = box.astype("int")
            # extract the face ROI
            face = image_pixeled[startY:endY, startX:endX]

            face = anonymize_face_pixelate(face, blocks)
            image_pixeled[startY:endY, startX:endX] = face

    return image_pixeled

def anonymize_face_pixelate(face_image, blocks=3):
    # divide the input image into NxN blocks
    (h, w) = face_image.shape[:2]
    xSteps = np.linspace(0, w, blocks + 1, dtype="int")
    ySteps = np.linspace(0, h, blocks + 1, dtype="int")

    # loop over the blocks in both the x and y direction
    for i in range(1, len(ySteps)):
        for j in range(1, len(xSteps)):
            # compute the starting and ending (x, y)-coordinates
            # for the current block
            startX = xSteps[j - 1]
            startY = ySteps[i - 1]
            endX = xSteps[j]
            endY = ySteps[i]

            # extract the ROI using NumPy array slicing, compute the
            # mean of the ROI, and then draw a rectangle with the
            # mean RGB values over the ROI in the original image
            roi = face_image[startY:endY, startX:endX]
            (B, G, R) = [int(x) for x in cv2.mean(roi)[:3]]
            cv2.rectangle(face_image, (startX, startY), (endX, endY),
                          (B, G, R), -1)

    # return the pixelated blurred image
    return face_image

    # output = np.hstack([image, image_pixeled])
    # cv2.imshow("Output", output)
    # cv2.waitKey(0)

def anonymize_base64_image(image_base64, preset_confidence, blocks=3):
    image_bytes = base64.b64decode(image_base64)
    image_array = np.frombuffer(image_bytes, dtype=np.uint8)
    image = cv2.imdecode(image_array, flags=cv2.IMREAD_COLOR)
    return anonymize_image(image, preset_confidence, blocks)


def anonymize_image_from_path(image_path, preset_confidence, blocks=3):
    image = cv2.imread(image_path)
    return anonymize_image(image, preset_confidence, blocks)



#anonymize_image("foto.jpg",preset_confidence=0.5, blocks=10)