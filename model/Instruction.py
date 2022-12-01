import re

class Instruction:

    def __init__(self, text) -> None:
        
        text_lis = re.sub(r"[(),.;@#?!&$]+\ *", " ", text).strip().split(" ")

        self.intruction_type = text_lis[0]
        self.parameter_lis = text_lis[1:]

    def get_instruction_type(self):

        return self.intruction_type

    def get_parameters(self):

        return self.parameter_lis
