import pandas as pd
import time


from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pymongo
from pymongo import MongoClient


class OpenSeaBot:
    def __init__(self):
        self.webdriver = webdriver.Chrome(ChromeDriverManager().install())
        self.webdriver.maximize_window()
        self.webdriver.get("https://opensea.io/rankings?sortBy=total_volume")

    def scrapePage(self, page):

        buttonSkip = self.webdriver.find_element(
            By.XPATH,
            "/html/body/div[1]/div[1]/main/div/div[3]/button[2]",
        )
        for _ in range(page):  # andare alla pagina target
            self.webdriver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            buttonSkip.click()

        action = webdriver.ActionChains(self.webdriver)
        self.webdriver.execute_script("window.scrollTo(0, 0)")
        scraped = []
        dataNFTs = []
        scroll = 200

        time.sleep(10)  # sleep per assicurarsi che tutto sia caricato

        while len(scraped) != 100:  # finchè non cattura 100 records

            # se non ne trova più e solleva un exception, scorre la pagina (react non carica tutti i componenti ma solo quelli visibili)
            try:
                i = 1
                while True:  # finchè trova records
                    # print(self.webdriver.find_element(By.XPATH,'/html/body/div[1]/div[1]/main/div/div[2]/div/div[2]/div['+str(i)+']/a/div[1]/div[3]/span/div').text)

                    currentNFTtitle = self.webdriver.find_element(
                        By.XPATH,
                        "/html/body/div[1]/div[1]/main/div/div[2]/div/div[2]/div["
                        + str(i)
                        + "]/a/div[1]/div[3]/span/div",
                    ).text
                    if currentNFTtitle not in scraped and currentNFTtitle != "":  # se non ha raccolto ancora questo record

                        currentNFTvolume = self.webdriver.find_element(
                            By.XPATH,
                            "/html/body/div[1]/div[1]/main/div/div[2]/div/div[2]/div["
                            + str(i)
                            + "]/a/div[2]/div/span/div",
                        ).text
                        currentNFTfloor = self.webdriver.find_element(
                            By.XPATH,
                            "/html/body/div[1]/div[1]/main/div/div[2]/div/div[2]/div["
                            + str(i)
                            + "]/a/div[5]/div/span/div",
                        ).text

                        currentNFTownersElement = self.webdriver.find_element(
                            By.XPATH,
                            "/html/body/div[1]/div[1]/main/div/div[2]/div/div[2]/div["
                            + str(i)
                            + "]/a/div[6]/p",
                        )

                        currentNFTitemsElement = self.webdriver.find_element(
                            By.XPATH,
                            "/html/body/div[1]/div[1]/main/div/div[2]/div/div[2]/div["
                            + str(i)
                            + "]/a/div[7]/p",
                        )

                        self.webdriver.execute_script(
                            "arguments[0].scrollIntoView();", currentNFTownersElement
                        )
                        self.webdriver.execute_script(
                            "window.scrollBy(0,-200)")
                        time.sleep(0.25)
                        # metti il cursore sul numero per mostrare la cifra precisa
                        action.move_to_element(currentNFTownersElement)

                        action.perform()
                        time.sleep(0.25)

                        try:

                            currentNFTowners = self.webdriver.find_element(
                                By.XPATH,
                                "/html/body/div[4]/div/div[1]/div",
                            ).text

                            action.move_to_element(currentNFTitemsElement)
                            action.perform()
                            time.sleep(0.25)

                            currentNFTitems = self.webdriver.find_element(
                                By.XPATH,
                                "/html/body/div[4]/div/div[1]/div",
                            ).text

                        except:
                            self.webdriver.execute_script(
                                "arguments[0].scrollIntoView();",
                                currentNFTownersElement,
                            )
                            time.sleep(0.25)
                            action.move_to_element(currentNFTownersElement)
                            action.perform()
                            time.sleep(0.25)
                            currentNFTowners = self.webdriver.find_element(
                                By.XPATH,
                                "/html/body/div[4]/div/div[1]/div",
                            ).text

                            action.move_to_element(currentNFTitemsElement)
                            action.perform()
                            time.sleep(0.25)

                            currentNFTitems = self.webdriver.find_element(
                                By.XPATH,
                                "/html/body/div[4]/div/div[1]/div",
                            ).text

                        self.webdriver.execute_script(
                            "window.scrollTo(0," + str(scroll - 200) + ")"
                        )

                        dataNFTs.append(  # aggiungi alla raccolta dei 100 della pagina
                            [
                                currentNFTtitle,
                                currentNFTvolume,
                                currentNFTfloor,
                                currentNFTowners,
                                currentNFTitems,
                            ]
                        )
                        print(currentNFTtitle + " added!")
                        print(dataNFTs[-1])
                        print(len(dataNFTs))
                        scraped.append(currentNFTtitle)
                        time.sleep(0.25)

                    else:
                        print(currentNFTtitle + " skipped!")

                    i += 1

            except:
                print(
                    "An exception occurred, "
                    + str(len(scraped))
                    + " out of 100 of this page collected..."
                )
                print("Scrolling down the page to find new NFT collections...")
                self.webdriver.execute_script(
                    "window.scrollTo(0," + str(scroll) + ")")
                scroll += 200

        return dataNFTs  # restituisci i 100


client = MongoClient('localhost', 27017)
db = client.DatMan
collection = db['nft']


i = 0  # pagina iniziale

while True:  # non sappiamo quante pagine ci siano effettivamente, interrompiamo quando abbiamo abbastanza dati

    bot = OpenSeaBot()  # inizializza l'istanza di chrome

    time.sleep(10)  # aspetta che tutto si carica

    currentNFTS = bot.scrapePage(i)  # fa lo scrape della pagina i esinma

    index = i*100

    dataframe = pd.DataFrame(currentNFTS, columns=["Collection", "Volume", "Floor Price", "Owners", "Items"]).reindex(
        range(index, index + 100))  # crea il dataset

    json = dataframe.to_dict(orient="record")

    collection.insert_many(json)

    # chiude la pagina web (questo per evitare il consumo eccessivo di memoria)
    bot.webdriver.quit()

    time.sleep(1)  # aspetta 1 secondo

    i += 1  # passa alla pagina successiva
