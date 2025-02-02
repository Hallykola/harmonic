class TradeSettings:
    def __init__(self,pair,obj) :
        self.pair = pair
        self.n_ma = obj["n_ma"]
        self.n_std = obj["n_std"]
        self.maxspread = obj["maxspread"]
        self.mingain = obj["mingain"]
        self.riskreward = obj["riskreward"]
        self.pip = obj["pip"]


    def __repr__(self):
        return str(vars(self))
    @classmethod
    def settings_to_str(cls, settings):
        ret_str = "Trade Settings:\n"
        for _, v in settings.items():
            ret_str += f"{v}\n"

        return ret_str
    # i dey wait you to see why you do am(settings_to_str) like this