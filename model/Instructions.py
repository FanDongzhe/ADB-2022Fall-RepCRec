import re


class Instructions:
    """
    instructions: An improperly handled raw string
    """
    ParametersMatch = "\((.*?)\)"

    def __init__(self, instructions):

        self.instruction_type = instructions.split('(')[0]
        self.instruction_type = self.instruction_type.strip(" ")

        self.parameters = re.search(self.ParametersMatch, instructions).group()
        self.parameters = self.parameters.strip('()')
        self.parameters = map(lambda x: x.strip(), self.params.split(','))


    def get_instruction_type(self):
        return self.instruction_type
        
    def get_parameters(self):
        return self.parameters

    
