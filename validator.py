class BodyTemperatureValidator():
    def __init__(self, min_val=35.0, max_val=42.0):
        self.min_val = min_val
        self.max_val = max_val

    def parse_and_validate(self, value):
        try:
            value = float(value)
        except ValueError as e:
            raise ValueError('入力内容が不正です。数値データを入力してください。')

        if value < self.min_val:
            raise ValueError('体温が低すぎます（もしかして冬眠中ですか？）。現実的な体温の値を入力してください。')
        if value > self.max_val:
            raise ValueError(
                '体温が高すぎます（もしかしてあなたはアヒルでしょうか？）。現実的な体温の値を入力してください。')

        return value
