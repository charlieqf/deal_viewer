import unittest
from text_quality import broken_character_map

class CharacterMapTests(unittest.TestCase):
    def test_corrupt_chinese_font_mapping(self):
        self.assertTrue(broken_character_map('ѝഭ䬦㹼㛑ԭᴹ䲀ޜਨ 2025 ᒤᓖ䍒ᣕ㺘䱴⌘ '*10))
        self.assertTrue(broken_character_map('᱙㵹ᠮ㐚Ⴙ઱⅁Ն䷻䮖᪝ᢛᏀ 2024 13.56 '*10))
    def test_valid_chinese_english_and_short_text(self):
        self.assertFalse(broken_character_map('中国银行股份有限公司资本充足率 19.47% '*20))
        self.assertFalse(broken_character_map('Consolidated financial statements for the year ended December 31 2025. '*20))
        self.assertFalse(broken_character_map('ⅠⅡⅢ 资产'))

if __name__=='__main__':unittest.main()
