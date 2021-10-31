import pandas as pd
import os

# this file contains variables and names given in turkish words
# blood transfusions related data

writer = pd.ExcelWriter('tümü.xlsx', engine='xlsxwriter')
writer2 = pd.ExcelWriter('ozet.xlsx', engine='xlsxwriter')
writer3 = pd.ExcelWriter('hasta başı toplam transfüzyon sayısı.xlsx', engine='xlsxwriter')

file_list = []
for file in os.listdir("veriler"):
    if file.endswith(".xls") or file.endswith(".xlsx"):
        file_list.append(file)
        continue
    else:
        continue
print(file_list)
hast_list = [s.strip('.xls') for s in file_list]
print(hast_list)

pivot = pd.DataFrame(columns=['HGB > 10', "Kanıtsız ES Sayı", "Son 24s Kanıtsız ES", 'Toplam ES Sayısı',
                              "PLT > 100.000", "Kanıtsız PLT Sayı", "Son 24s Kanıtsız PLT", "Toplam PLT Trans.",
                              "INR < 1,5", "Kanıtsız TDP Sayı", "Son 24s Kanıtsız TDP", "Top TDP Trans.",
                              "Toplam End. Dışı", "Toplam Kanıtsız", " Toplam Son 24s Kanıtsız", "Toplam Transfüzyon",
                              "Toplam Hasta Sayısı"], index=hast_list)

for adi in hast_list:
    print(f'Şu an {adi} hastanesi işlenmekte...')
    kan_ham_tablo = pd.read_excel("veriler/" + adi + ".xlsx", dtype='object')
    kan_ham_tablo['Çıkış Tarihi'] = pd.DatetimeIndex(kan_ham_tablo['Çıkış Tarihi'], dayfirst=True)
    print(kan_ham_tablo.info())

    hasta_sayisi = kan_ham_tablo['Dosya No'].nunique()
    toplam_transfuzyon = kan_ham_tablo['Dosya No'].count()

    kan_ham_tablo['HGB'] = kan_ham_tablo["Geçmiş"].str.extract(r'(HGB = \d+\.\d+|HGB = \d+)', expand=False)
    kan_ham_tablo['HGB Değer'] = kan_ham_tablo["HGB"].str.extract(r'(\d+\.\d+|\d+)', expand=False)
    kan_ham_tablo['PLT'] = kan_ham_tablo["Geçmiş"].str.extract(r'(PLT = \d+\.\d+|PLT = \d+)', expand=False)
    kan_ham_tablo['PLT Değer'] = kan_ham_tablo["PLT"].str.extract(r'(\d+\.\d+|\d+)', expand=False)
    kan_ham_tablo['aPTT'] = kan_ham_tablo["Geçmiş"].str.extract(r'(aPTT = \d+\.\d+|aPTT = \d+)', expand=False)
    kan_ham_tablo['aPTT Değer'] = kan_ham_tablo["aPTT"].str.extract(r'(\d+\.\d+|\d+)', expand=False)
    kan_ham_tablo['PT'] = kan_ham_tablo["Geçmiş"].str.extract(r'(PT = \d+\.\d+|PT = \d+)', expand=False)
    kan_ham_tablo['PT Değer'] = kan_ham_tablo["PT"].str.extract(r'(\d+\.\d+|\d+)', expand=False)
    kan_ham_tablo['INR'] = kan_ham_tablo["Geçmiş"].str.extract(r'(INR = \d+\.\d+|INR = \d+)', expand=False)
    kan_ham_tablo['INR Değer'] = kan_ham_tablo["INR"].str.extract(r'(\d+\.\d+|\d+)', expand=False)

    kan_ham_tablo['HGB Değer'] = kan_ham_tablo['HGB Değer'].astype(float)
    kan_ham_tablo['PLT Değer'] = kan_ham_tablo['PLT Değer'].astype(float)
    kan_ham_tablo['aPTT Değer'] = kan_ham_tablo['aPTT Değer'].astype(float)
    kan_ham_tablo['PT Değer'] = kan_ham_tablo['PT Değer'].astype(float)
    kan_ham_tablo['INR Değer'] = kan_ham_tablo['INR Değer'].astype(float)

    kayit_icin = kan_ham_tablo.drop(["HGB", "PLT", "aPTT", "PT", "INR"], 1)
    kayit_icin.to_excel(writer, sheet_name=adi)

    pivot_hasta = pd.pivot_table(kayit_icin, values='Kan Ürünü Cinsi', index='Dosya No', aggfunc='count')
    pivot_hasta = pivot_hasta.sort_values(by='Kan Ürünü Cinsi', ascending=False)
    pivot_hasta.to_excel(writer3, sheet_name=adi)

    kayit_icin['gecmis_tarih'] = kayit_icin["Geçmiş"].str.extract(r'(\d+\.\d+.\d+ \d+:\d+)', expand=False)
    kayit_icin['gecmis_tarih'] = pd.DatetimeIndex(kayit_icin['gecmis_tarih'], dayfirst=True)
    kayit_icin['tarih_fark'] = kayit_icin['Çıkış Tarihi'] - kayit_icin['gecmis_tarih']

    hgb_trans_toplam = kayit_icin[kayit_icin['Kan Ürünü Cinsi'].str.contains('ritrosit')]
    hgb_end_disi = hgb_trans_toplam[hgb_trans_toplam['HGB Değer'] > 10]
    hgb_end_disi = len(hgb_end_disi)
    hgb_no_kanit = hgb_trans_toplam[~hgb_trans_toplam["Geçmiş"].str.contains('HGB', na=False)]
    hgb_no_kanit = len(hgb_no_kanit)
    hgb_dolu_gecmis = hgb_trans_toplam[hgb_trans_toplam["Geçmiş"].str.contains('HGB', na=False)]
    hgb_date_diff = hgb_dolu_gecmis[hgb_dolu_gecmis['tarih_fark'] > pd.Timedelta(days=1)]
    print(hgb_date_diff)
    hgb_no_kanit_24 = len(hgb_date_diff)
    hgb_trans_toplam = len(hgb_trans_toplam)
    if hgb_trans_toplam == 0:
        hgb_oran = 0
    else:
        hgb_oran = hgb_end_disi / hgb_trans_toplam

    plt_trans_toplam = kayit_icin[kayit_icin['Kan Ürünü Cinsi'].str.contains('rombosit|PLT')]
    plt_end_disi = plt_trans_toplam[plt_trans_toplam['PLT Değer'] > 100]
    plt_end_disi = len(plt_end_disi)
    plt_no_kanit = plt_trans_toplam[~plt_trans_toplam["Geçmiş"].str.contains('PLT', na=False)]
    plt_no_kanit = len(plt_no_kanit)
    plt_dolu_gecmis = plt_trans_toplam[plt_trans_toplam["Geçmiş"].str.contains('PLT', na=False)]
    plt_date_diff = plt_dolu_gecmis[plt_dolu_gecmis['tarih_fark'] > pd.Timedelta(days=1)]
    print(plt_date_diff)
    plt_no_kanit_24 = len(plt_date_diff)
    plt_trans_toplam = len(plt_trans_toplam)
    if plt_trans_toplam == 0:
        plt_oran = 0
    else:
        plt_oran = plt_end_disi / plt_trans_toplam

    inr_trans_toplam = kayit_icin[kayit_icin['Kan Ürünü Cinsi'].str.contains('lazma')]
    inr_end_disi = inr_trans_toplam[inr_trans_toplam['INR Değer'] < 1.5]
    inr_end_disi = len(inr_end_disi)
    inr_no_kanit = inr_trans_toplam[~inr_trans_toplam["Geçmiş"].str.contains('INR', na=False)]
    inr_no_kanit = len(inr_no_kanit)
    inr_dolu_gecmis = inr_trans_toplam[inr_trans_toplam["Geçmiş"].str.contains('INR', na=False)]
    inr_date_diff = inr_dolu_gecmis[inr_dolu_gecmis['tarih_fark'] > pd.Timedelta(days=1)]
    print(inr_date_diff)
    inr_no_kanit_24 = len(inr_date_diff)
    inr_trans_toplam = len(inr_trans_toplam)
    if inr_trans_toplam == 0:
        inr_oran = 0
    else:
        inr_oran = inr_end_disi / inr_trans_toplam

    toplam_transfuzyon = hgb_trans_toplam + plt_trans_toplam + inr_trans_toplam
    toplam_end_disi = hgb_end_disi + plt_end_disi + inr_end_disi
    toplam_no_kanit = hgb_no_kanit + plt_no_kanit + inr_no_kanit
    toplam_no_kanit_24 = hgb_no_kanit_24 + plt_no_kanit_24 + inr_no_kanit_24

    pivot.loc[adi] = [hgb_end_disi, hgb_no_kanit, hgb_no_kanit_24, hgb_trans_toplam,
                      plt_end_disi, plt_no_kanit, plt_no_kanit_24, plt_trans_toplam,
                      inr_end_disi, inr_no_kanit, inr_no_kanit_24, inr_trans_toplam,
                      toplam_end_disi, toplam_no_kanit, toplam_no_kanit_24, toplam_transfuzyon,
                      hasta_sayisi]

pivot.to_excel(writer2, sheet_name="orj")

print(pivot)
writer.save()
writer2.save()
writer3.save()

"""fig, ax = plt.subplots(1, 3)
sns.set_style("darkgrid")
sns.barplot(ax=ax[0], x=pivot.index, y="Ref. Dışı ES Oran", data=pivot)
sns.barplot(ax=ax[1], x=pivot.index, y="Ref. Dışı PLT Oran", data=pivot)
sns.barplot(ax=ax[2], x=pivot.index, y="Ref. Dışı TDP Oran", data=pivot)

plt.show()"""
print("Tamamlanıyor...")
exit()
