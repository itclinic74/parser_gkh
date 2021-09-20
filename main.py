import requests, shutil
from bs4 import BeautifulSoup as BS
from fake_useragent import UserAgent
import zipfile
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from decl_alchemy import Reestr
from config import host, user, password, db_name, port

HOST = 'https://www.reformagkh.ru'
URL = 'https://www.reformagkh.ru/opendata'
HEADERS = {
    'user-agent': UserAgent().chrome
}
PARAMS = 'gid=2280999&page=2'
db = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(user, password, host, port, db_name)
engine = create_engine(db)
session = sessionmaker(bind=engine)
s = session()


def get_session():
    session = requests.Session()
    return session


def get_content(html):
    soup = BS(html, 'html.parser')
    ruo_Moscow = soup.select('.lh-27')
    for el in ruo_Moscow:
        if el.text == 'Реестр домов по городу Москве':
            element = el.find_next('div')
            items = element.select('.my-3')
            break
        else:
            continue
    for el in items:
        link = el.find('a')
    link = link.get('href')
    link = HOST + link
    return link


def download_file(link):
    filereq = requests.get(link, stream=True)
    file = 'Reestr'
    with open(file, "wb") as receive:
        shutil.copyfileobj(filereq.raw, receive)
    del filereq
    return file


def csv_dict_reader(file_obj):
    reader = csv.DictReader(file_obj, delimiter=';')
    i = 1
    for row in reader:
        citizen = ''
        id_gkh = row["\ufeffid"]
        url_home = 'https://www.reformagkh.ru/myhouse/profile/passport/' + id_gkh
        html = get_session().get(url=url_home, headers=HEADERS, params=0)
        soup = BS(html.text, 'html.parser')
        print(i)
        i += 1
        specification = soup.select('.mr-5')
        for el in specification:
            if el.text == 'Численность жителей, чел.':
                element = el.find_next('div')
                try:
                    citizen = element[0].text
                    print(citizen)
                except:
                    break
                break
            else:
                continue
        item = Reestr(
            id_gkh=row["\ufeffid"],
            region_id=row["region_id"],
            area_id=row["area_id"],
            city_id=row["city_id"],
            street_id=row["street_id"],
            shortname_region=row["shortname_region"],
            formalname_region=row["formalname_region"],
            shortname_area=row["shortname_area"],
            formalname_area=row["formalname_area"],
            shortname_city=row["shortname_city"],
            formalname_city=row["formalname_city"],
            shortname_street=row["shortname_street"],
            formalname_street=row["formalname_street"],
            house_number=row["house_number"],
            building=row["building"],
            block=row["block"],
            letter=row["letter"],
            address=row["address"],
            houseguid=row["houseguid"],
            management_organization_id=row["management_organization_id"],
            built_year=row["built_year"],
            exploitation_start_year=row["exploitation_start_year"],
            project_type=row["project_type"],
            house_type=row["house_type"],
            is_alarm=row["is_alarm"],
            method_of_forming_overhaul_fund=row["method_of_forming_overhaul_fund"],
            floor_count_max=row["floor_count_max"],
            floor_count_min=row["floor_count_min"],
            entrance_count=row["entrance_count"],
            elevators_count=row["elevators_count"],
            energy_efficiency=row["energy_efficiency"],
            quarters_count=row["quarters_count"],
            living_quarters_count=row["living_quarters_count"],
            unliving_quarters_count=row["unliving_quarters_count"],
            area_total=row["area_total"],
            area_residential=row["area_residential"],
            area_non_residential=row["area_non_residential"],
            area_common_property=row["area_common_property"],
            area_land=row["area_land"],
            parking_square=row["parking_square"],
            playground=row["playground"],
            sportsground=row["sportsground"],
            other_beautification=row["other_beautification"],
            foundation_type=row["foundation_type"],
            floor_type=row["floor_type"],
            wall_material=row["wall_material"],
            basement_area=row["basement_area"],
            chute_type=row["chute_type"],
            chute_count=row["chute_count"],
            electrical_type=row["electrical_type"],
            electrical_entries_count=row["electrical_entries_count"],
            heating_type=row["heating_type"],
            hot_water_type=row["hot_water_type"],
            cold_water_type=row["cold_water_type"],
            sewerage_type=row["sewerage_type"],
            sewerage_cesspools_volume=row["sewerage_cesspools_volume"],
            gas_type=row["gas_type"],
            ventilation_type=row["ventilation_type"],
            firefighting_type=row["firefighting_type"],
            drainage_type=row["drainage_type"],
            citizen_count=citizen
        )
        s.add(item)
    s.commit()


def parse():
    html = get_session().get(url=URL, headers=HEADERS, params=PARAMS)
    link = get_content(html.text)
    # Скачиваем архив
    filename = download_file(link)
    # Извлекаем содержимое
    if zipfile.is_zipfile(filename):
        z = zipfile.ZipFile(filename, 'r')
        z.extractall()
        # Читаем .csv
        csv_path = z.filelist[0].filename
    with open(csv_path, "r") as f_obj:
        csv_dict_reader(f_obj)


if __name__ == '__main__':
    parse()
