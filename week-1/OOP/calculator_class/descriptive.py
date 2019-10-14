import statistics as stats

class Calculator:

    def __init__(self, data = []):
        self.data = data

    @property
    def length(self):
        return len(self.data)

    @property
    def mean(self):
        return stats.mean(self.data)


    @property
    def median(self):
        return stats.median(self.data)

    @property
    def variance(self):
        return stats.pvariance(self.data)

    @property
    def stand_dev(self):
        return stats.pstdev(self.data)

    def add_data(self, new_data):
        if isinstance(new_data, list):
            self.data.extend(new_data)
        else:
            self.data.append(new_data)

    def remove_data(self, to_remove):
        if not isinstance(new_data, list):
            self.remove_data([to_remove])
        for item in to_remove:
            self.data.remove(item)


# .median
# .variance
# .stand_dev
# Your class should have the following methods:
#
# .add_data() - which can take in a value or a list of values and extend the .data attribute
# .remove_data() accept a list of numbers and remove any of the numbers in that list from your object data
# Bonus:
#
# - calc_correlation(second_variable) this method should accept a list of data for the second variable,
# and then calculate the correlation coefficient for the two variables.
