from __future__ import annotations

import unittest

from scripts.name_detector import (
    CompanyNameDetector,
    FundNameDetector,
    NameEquivalence,
    ProductLabelDetector,
)


class ProductLabelDetectorTests(unittest.TestCase):
    def test_detects_structured_product_labels(self) -> None:
        self.assertTrue(ProductLabelDetector.is_product_label("BPA TB LG GOOG 1629 B1629 OP EN"))
        self.assertTrue(ProductLabelDetector.is_product_label("UCH AAPL/SALESFORCE/ZOOM AIR 20"))
        self.assertTrue(ProductLabelDetector.is_product_label("UCH BONUS CAP STLA 7,413 108 17"))
        self.assertTrue(ProductLabelDetector.is_product_label("LYFT INC LYFT ORD  CLASS A (CDI"))

    def test_allows_real_company_names(self) -> None:
        self.assertFalse(ProductLabelDetector.is_product_label("AGNC Investment Corp."))
        self.assertFalse(ProductLabelDetector.is_product_label("Guidewire Software, Inc."))
        self.assertFalse(ProductLabelDetector.is_product_label("Salesforce, Inc."))

    def test_company_name_detection(self) -> None:
        positives = [
            "AGNC Investment Corp.",
            "Betsson AB",
            "Guidewire Software, Inc.",
            "Salesforce, Inc.",
            "Lyft Inc.",
            "Global Holdings Ltd",
            "ANC Investment Group PLC",
            "Nordic Bank AB",
            "Bayerische Motoren Werke AG",
            "UnitedHealth Group Incorporated",
            "Samsung Electronics Co., Ltd",
            "Alphabet Inc.",
            "Tencent Holdings Ltd",
            "Stellantis N.V.",
            "Daimler Truck Holding AG",
            "Cruise Acquisition Corp",
            "BlackRock, Inc.",
            "Visa Inc.",
            "Mastercard Incorporated",
            "NVIDIA Corporation",
            "Space Exploration Holdings, LLC",
            "Oracle Corporation",
            "Johnson & Johnson",
            "Procter & Gamble Co.",
            "Walmart Inc.",
            "Amazon.com, Inc.",
            "Tesla, Inc.",
            "Meta Platforms, Inc.",
            "Adobe Inc.",
            "IBM Corporation",
            "Cisco Systems, Inc.",
            "General Electric Company",
            "The Goldman Sachs Group, Inc.",
            "American Airlines Group Inc.",
            "Delta Air Lines, Inc.",
            "Marriott International, Inc.",
            "Booking Holdings Inc.",
            "Airbus SE",
            "Ryanair Holdings plc",
            "Novo Nordisk A/S",
            "ASML Holding N.V.",
            "SAP SE",
            "Allianz SE",
            "LVMH Moët Hennessy Louis Vuitton SE",
            "Visa Europe Services LLC",
            "BP p.l.c.",
            "Eni S.p.A.",
            "Reliance Industries Limited",
            "Infosys Limited",
            "Tata Consultancy Services Limited",
            "HDFC Bank Limited",
            "ICICI Bank Limited",
            "Banco Santander S.A.",
            "BBVA SA",
            "Deutsche Telekom AG",
            "Vodafone Group Plc",
            "Betterware de Mexico, S.A.B. de C.V.",
            "Sewoo Global Co., Ltd",
            "InDex Pharmaceuticals Holding AB (publ)",
            "Intercontinental International Real Estate Investment Company",
        ]
        negatives = [
            "BPA TB LG GOOG 1629 B1629 OP EN",
            "LYFT INC LYFT ORD  CLASS A (CDI",
            "EDF4%PT",
            "UKH BET 200 CALL 123",
            "ETF SPDR S&P500",
            "CDI MSFT 340",
            "BETS-B.ST",
            "V72H.F",
            "BPA CW CALL INTESA 1.6 A",
            "UCH TB LG GOOG 1629 B1629 OP EN",
            "0QYJ.IL",
            "0XNH.L",
            "0Y0Y.L",
            "UV4L19.MI",
            "UV4L1X.MI",
            "TROO",
            "2CRM.L",
            "2G2.SG",
            "0GS.F",
            "0RIX.L",
            "1111-HR CALL OPTION",
            "CDI-APPLE-USD",
            "UKH BONUS CAP STLA 7,413 108 17",
            "BONDS 5Y FLOATING",
            "SPX500-CL-USD",
            "FUND-AGR-3YR",
            "EDE45%Y",
            "ETF-QQQ-USD",
            "BMSH-CO-ETF",
            "BPA TB LG AMAZON 3200 A",
            "BETA-CALL-200",
            "JUMBO-CW 2025",
            "TFB%352",
            "CDI-LVGI 100",
            "GAZPROM FORTNIGHTLY",
            "NIO CW CALL 120 A",
            "UBI BOND 8%",
            "BONUS CAP STLA 7,413 108 17",
            "UBS ETF CH",
            "BPA TB LG GOOG 1629",
            "UK-ETF-RET-2028",
            "ETF-CASH-TR-ON",
            "BPA BONUS CAP 7.5",
            "BOND 10Y ICT",
            "CASHCOLWO AMBYFN 70 5,75 19",
            "UCE CALL BMW 100 A",
            "UCH TB SH EURO 1640",
            "PHYSICAL GOLD 1OZ",
            "SPX-EU INDEX TR",
            "BR-NAVY-ETF",
            "% return swap 2yr",
            "Ambitions Enterprise Management Co. L.L.C Class A Ordinary Shares",                                
            "Robo.ai Inc. Class B Ordinary Shares",                                                             
            "Advanced Health Intelligence Ltd. American Depositary Shares",                                     
            "Australian Oilseeds Holdings Limited Ordinary Shares",                                             
            "AgomAb Therapeutics NV American Depositary Shares",                                                
            "company  Ascentage Pharma Group International American Depository Shares When Issued",             
            "Atour Lifestyle Holdings Limited American Depositary Shares",                                      
            "CHINATRUST SECS CO LTD C/W 03/0",                                                                  
            "Robo.ai Inc. Warrant",                                                                             
            "CHINATRUST SECS CO LTD C/W 03/1",                                                                  
            "CHINASOFT INTL LTD HD-,05",                                                                        
            "ChinaAMC Pansheng Flx Alloc(LOF)",                                                                 
            "CHOW SANG SANG LTD HD-,25",                                                                        
            "Christchurch City Holdings Limited 3.4% BDS 06/12/22 NZD5000",                                     
            "Christchurch International Airport Limited 4.13% BDS 24/05/24 NZD5000",                            
            "Churchill Capital Corp X Unit",                                                                    
            "Churchill Capital Corp X Warrants",                                                                
            "Churchill Capital Corp XI Units",                                                                  
            "Churchill Capital Corp XI Warrants",                                                               
            "Ciena Corp. Registered Shares N",                                                                  
            "Cigna Corp. Registered Shares D",                                                                  
            "Citizens Financial Group Inc. Depositary Shares Each Representing 1/40th Interest in a Share of 5.000% Fixed-Rate Non-Cumulative Perpetual Preferred Stock Series E",
            "Citizens Financial Group Inc. Depositary Shares each representing a 1/40th Interest in a Share of 6.350% Fixed-to-Floating Rate Non-Cumulative Perpetual Preferred Stock Series D",
            "Citizens Financial Group Inc. Depositary Shares Each Representing a 1/40th Interest in a Share of 7.375% Fixed-Rate Non-Cumulative Perpetual Preferred Stock Series H",
            "BAYERISCHE MOTOREN WERKE AG VOR",
            "BAYERISCHE MOTOREN WERKE AG STA",
            "Bayerische Motoren Werke AG Nam",
            "Eyemaxx Real Estate AG Inhaber-",
            "BranchOut Food Inc. Common Stock",
            "Boer Power Holdings Ltd. Regist",
            "Berry Global Group Inc. Registe",
            "Bastei Lbbe AG Inhaber-Aktien o",
            "Turning Point Brands Inc. Regis",
            "Onconova Therapeutics Inc. Regi",
            "It Now Id ETF Ima-B Fundo De Indice",
            "SB Corn ETN",
            "BOCOM SCHRODERS FUND MGMT CO LT",
            "BROOKFIELD ASSET MGMT INC PREF",
            "ELEMENT FLEET MGMT CORP PREF SE",
            "Eastern Water Res Dev&Mgmt PCLR",
            "Eastern Water Res Dev&Mgmt PCLR",
            "Advanced Inf.Serv.(ADVANC) PCLR",
            "Ameris Capital Administradora General de Fondos S.A.- Fundo de Inversion Ameris DVA All Cap Chile Fu",
            "Ameris Capital Administradora General de Fondos S.A. - Ameris Mc Renta Industrial Fondo De Inversion",
            "Ameris Capital Administradora General de Fondos S.A. - Ameris Carteras Comerciales Fondo de Inversio",
            "Ameris Capital Administradora General De Fondos S.A. - Ameris LGT CCO II Fondo De Inversion",
            "Ameris Capital Administradora General De Fondos S.A. - Ameris Finance Corto Plazo Fondo De Inversion",
            "Ameris Capital Administradora General De Fondos S.A. - Ameris Dva Silicon Fund Fondo De Inversion",
            "Ameris Cap Administra Gen De Fondos SA - Ameris Deuda con Garantia Hipotecaria Fondo de Inversion",
            "American Water Works Co. Inc. R",
            "ALLG.GOLD U.SILBER.AG O.N",
            "ALLERTHAL-WERKE-AG O.N.",
            "Waran Seri I Zyrexindo Mandiri",
            "Ascencio S.C.A. Actions Nom. o.",
            "COMMERZBK AG SPONS.ADR",
            "China ZhengTong Auto Svcs Hld.R",
            "Leroy Seafood Group AS Navne-Ak",
            "Yanzhou Coal Mining Co. Ltd. Re",
            "Yue Yuen Indust.(Hldgs) Ltd. Re",
            "YOKOHAMA RUBBER CO. LTD., THE R",
            "Wstenrot& Wrttembergische AGNAM",
            "WisdomTree WTI Crude Oil Pre-roll",
            "Wereldhave Belgium SCA Actions",
            "WILH. WILHELMSEN HOLDING ASA Na",
            "Wealth Invest Linde &amp; Partners Dividende Fond",
            "Wealth Invest Linde &amp; Partners Global Value Fond",
            "Wealth Invest Alm Brand RentePlus",
        ]
        for name in positives:
            self.assertTrue(CompanyNameDetector.is_company_name(name), msg=name)
        for name in negatives:
            self.assertFalse(CompanyNameDetector.is_company_name(name), msg=name)


class FundNameDetectorTests(unittest.TestCase):
    def test_detects_fund_entities(self) -> None:
        positives = [
            "Vanguard Total Stock Market ETF",
            "ABC Income Fund",
            "Global Bond Trust",
            "S&P 500 Index",
            "Dividend Strategy Fund",
            "Total Return Market Fund",
            "BlackRock Smaller Companies Trust",
            "Barwa Real Estate Company Q.P.S.C.",
            "BOCOM SCHRODERS FUND MGMT CO LT",
            "FC WERTMGMT COLL.GS33",
            "Ameris Capital Administradora General de Fondos S.A.- Fundo de Inversion Ameris DVA All Cap Chile Fu",
            "Wealth Invest Linde &amp; Partners Dividende Fond",
            "Wealth Invest Linde &amp; Partners Global Value Fond",
            "Wealth Invest Alm Brand RentePlus",
        ]
        negatives = [
            "Delta Air Lines Inc",
            "Alphabet Inc",
            "Bayerische Motoren Werke AG",
            "General Motors",
            "Acme Holdings LLC",
            "Independent Global Akk",
            "It Now Id ETF Ima-B Fundo De Indice",
            "InDex Pharmaceuticals Holding AB (publ)",
            "UNIPROF REAL ESTATE HOLDING AGI",
            "UNIPROF REAL ESTATE HOLDING AG",
            "SB Corn ETN",
            "BROOKFIELD ASSET MGMT INC PREF",
            "ELEMENT FLEET MGMT CORP PREF SE",
            "Eastern Water Res Dev&Mgmt PCLR",
            "Eastern Water Res Dev&Mgmt PCLR",
            "Advanced Inf.Serv.(ADVANC) PCLR",
            "American Water Works Co. Inc. R",
            "ALLG.GOLD U.SILBER.AG O.N",
            "ALLERTHAL-WERKE-AG O.N.",
            "Waran Seri I Zyrexindo Mandiri",
            "Ascencio S.C.A. Actions Nom. o.",
            "COMMERZBK AG SPONS.ADR",
            "China ZhengTong Auto Svcs Hld.R",
            "Leroy Seafood Group AS Navne-Ak",
            "Yanzhou Coal Mining Co. Ltd. Re",
            "Yue Yuen Indust.(Hldgs) Ltd. Re",
            "YOKOHAMA RUBBER CO. LTD., THE R",
            "Wstenrot& Wrttembergische AGNAM",
            "WisdomTree WTI Crude Oil Pre-roll",
            "Wereldhave Belgium SCA Actions",
            "WILH. WILHELMSEN HOLDING ASA Na",
        ]
        for name in positives:
            self.assertTrue(FundNameDetector.is_fund_name(name), msg=name)
        for name in negatives:
            self.assertFalse(FundNameDetector.is_fund_name(name), msg=name)


class NameEquivalenceTests(unittest.TestCase):
    def test_matches_variants(self) -> None:
        self.assertTrue(
            NameEquivalence.names_match(
                "Barwa Real Estate Company Q.P.S.C.",
                "Barwa Real Estate Company qpsc",
            ),
            "should ignore punctuation and case",
        )
        self.assertTrue(
            NameEquivalence.names_match(
                "Barwa Real Estate co. Q.P.S.C.",
                "Barwa Real Estate Company qpsc",
            )
        )
        self.assertFalse(
            NameEquivalence.names_match(
                "Barwa Real Estate Company",
                "Different Entity LLC",
            )
        )
        self.assertFalse(
            NameEquivalence.names_match(
                "UNIPROF REAL ESTATE HOLDING AGI",
                "UNIPROF Real Estate Holding AG",
            )
        )


if __name__ == "__main__":
    unittest.main()
