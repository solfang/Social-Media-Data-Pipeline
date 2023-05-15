import pandas as pd
import os
from tqdm import tqdm

tqdm.pandas()  # makes .progress_apply() available

from deep_translator import GoogleTranslator
from deep_translator.exceptions import NotValidPayload
import spacy  # +run: python -m spacy download en_core_web_sm
from spacy.language import Language
from spacy_langdetect import LanguageDetector
import swifter
from requests.exceptions import ConnectionError


# needed for instantiation, see https://stackoverflow.com/questions/66712753/how-to-use-languagedetector-from-spacy-langdetect-package
def get_lang_detector(nlp, name):
    return LanguageDetector()


nlp = spacy.load("en_core_web_sm")
try:
    Language.factory("language_detector", func=get_lang_detector)
except ValueError:
    pass  # factory already instantiated
nlp.add_pipe('language_detector', last=True)


class Translator:
    """
    Detects the language and translates text in a dataframe column
    """

    def __init__(self, input_path, output_path, target_column, target_language, skip_if_exists=False):
        """
        :param input_path: input file path (file should be a csv)
        :param output_path: output file path (file should be a csv)
        :param target_column: column that will be translated
        :param target_language: shortcode for the language to translate into (e.g. English='en')
        :param skip_if_exists: skip if the translation was already run on the input file previously
        """
        self.df = pd.read_csv(input_path)
        self.output_path = output_path
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        self.target_column = target_column
        self.target_language = target_language
        self.skip_if_exists = skip_if_exists

    def run(self):

        # Check if the output file already exists
        if os.path.exists(self.output_path):
            if self.skip_if_exists:
                print("Output file already exists. Skipping. Output file at {}".format(self.output_path))
                return
            else:
                # Read in last checkpoint
                self.df = pd.read_csv(self.output_path)
        # Start new translation by detecting the language
        else:
            self.df = self.detect_language(self.df)

        self.df = self.translate_column(self.df, self.output_path)
        self.df.to_csv(self.output_path, index=True)
        print("Output table saved to {}".format(self.output_path))

    def detect_language(self, df) -> pd.DataFrame:
        def lang_detect(text):
            """
            Detects the 'main' language of a given text.
              Unfortunately, this is not very reliable (even though it uses spacy, which uses Google's langdetect https://github.com/Mimino666/langdetect)
              The built-in language detection in e.g. Google Translate seems to be more accurate, but it's not free.
              While language detection with a translator is not free, translating is (through some libraries).
              Therefore you should translate all sentences that are not already detected as the target language with high confidence (which lets the translator handle all fuzzy sentences)
            :param text: a string
            :return: tuple of: (language, confidence score)
            """
            if text.isspace() or not len(text):
                return "empty", 1.0
            doc = nlp(text)
            res = doc._.language  # looks like: {'language': 'en', 'score': 0.9999955763665352}
            return res["language"], res["score"]

        # replace NAs with ""
        df[self.target_column] = df[self.target_column].replace(pd.NA, "").astype(str)
        # detect language
        print("Detecting [{}] language".format(self.target_column))
        lang_tuples = df[self.target_column].swifter.apply(lang_detect)
        # turn (language, score) tuples into 2 separate lists
        lang, score = list(zip(*lang_tuples.tolist()))
        # add new language and score as new columns to the dataframe
        df["lang_og"] = lang
        df["lang_score"] = score
        return df

    def translate_column(self, df, fpath, min_score=0.9) -> pd.DataFrame:
        """
        Translates text in a target column into a target language
        >Requires having run the detect_language() function on the df first.
        Translation limitations: Text must be <5k characters (which seems to be extremely rare)
        :param fpath: where to save the dataframe to (save every n translations in case there are rate limits etc.)
        :param language: language shortcut, e.g. 'en'
        :param min_score: if a caption is detected as the target language with confidence score > min_score, it is not translated
        """

        target_language = self.target_language

        assert self.target_column in df.columns
        assert "lang_og" in df.columns and "lang_score" in df.columns

        translation_col = self.target_column + "_" + target_language
        # translation column may exist from an earlier (incomplete) run
        if translation_col not in df:
            df[translation_col] = pd.NA
        g_translator = GoogleTranslator(source='auto', target=target_language)
        print("Translating [{}] to {}".format(self.target_column, target_language))
        print("total: {}, to translate: {}, of which not translated yet: {} ".format(len(df), sum(
            (df["lang_og"] != target_language) | (df["lang_score"] <= min_score)), df[translation_col].isna().sum()))

        # Translate the column row by row
        for i, (idx, row) in enumerate(tqdm(df.iterrows(), total=len(df))):
            text, lang, score, translation = row[self.target_column], row["lang_og"], row["lang_score"], row[translation_col]

            # only translate if there's not already a translation from past executions
            if pd.isna(translation):
                # only translate if the caption language is detected as English with high probability
                if (lang == target_language and score > min_score) or (lang == "empty"):
                    translation = text
                else:
                    try:
                        translation = g_translator.translate(text)
                        # time.sleep(0.1)
                    except NotValidPayload as e:
                        print("text:", text)
                        print(e)
                        translation = "<error>"
                    except ConnectionError as e:
                        print(e)
                        translation = None
                df.loc[idx, translation_col] = translation

            # save df after 100 iterations so translation can be interrupted and resumed later (in case there are quota limits)
            if i % 100 == 0:
                df.to_csv(fpath, index=True)
        return df
