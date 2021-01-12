from pymongo import MongoClient
import json
import tkinter as tk
from tkinter import filedialog
import subprocess
import os

# use DBLP
# db.createCollection("publis")

class Connecteur:
    #Se connecte à la bd MongoDB
    @classmethod
    def connexion(cls):
        cls.client = MongoClient()
        db = cls.client.DBLP
        cls.col = db.publis

    #Se deconnecte à la bd MongoDB
    @classmethod
    def deconnexion(cls):
        cls.client.close()

    #Compte le nombre de documents de la collection publis
    @classmethod
    def count_db(cls):
        cls.connexion()
        result = cls.col.find()
        cls.deconnexion()
        return result.count()

    #Liste tous les livres (type “Book”)
    @classmethod
    def list_books(cls):
        liste = []
        cls.connexion()
        post = cls.col.find({'type': 'Book'},{'_id':0, 'title':1})
        for book in post:
            liste.append(book)
        cls.deconnexion()
        return liste

    #Liste les livres depuis 2014 
    @classmethod
    def after_2014(cls):
        liste = []
        cls.connexion()
        post = cls.col.find({'year': {'$gte' : 2014}}, {'_id':0, 'title':1, 'year': 1})
        for book in post:
            liste.append(book)
        cls.deconnexion()
        return liste

    #Liste les publications de l’auteur “Toru Ishida”
    @classmethod
    def publi_Toru(cls):
        liste = []
        cls.connexion()
        post = cls.col.find({'authors': 'Toru Ishida'}, {'_id':0, 'title':1})
        for book in post:
            liste.append(book)
        cls.deconnexion()
        return liste

    #Liste tous les auteurs distincts
    @classmethod
    def authors(cls):
        liste = []
        cls.connexion()
        post = cls.col.find({}, {'_id':0, 'authors':1})
        for list_authors in post:
            for authors in list_authors:
                for author in list_authors[authors]:
                    liste.append(author)
        cls.deconnexion()
        return set(liste)

    #Trie les publications de “Toru Ishida” par titre de livre
    @classmethod
    def sort_Toru(cls):
        liste = cls.publi_Toru()
        liste_sort = []
        for book in liste:
            liste_sort.append(book['title'])
            liste_sort.sort()
        return liste_sort

    #Compte le nombre de ses publications
    @classmethod
    def count_book_Toru(cls):
        cls.connexion()
        post = cls.col.find({'authors': 'Toru Ishida'}, {'_id':0, 'title':1}).count()
        return post

    #Compte le nombre de publications depuis 2011 et par type
    @classmethod
    def count_2001_type(cls):
        cls.connexion()
        post = cls.col.find({'year': {'$gte' : 2011}}, {'_id':0, 'type': 1})
        count = post.count()
        cls.deconnexion() 
        type_count = {}
        for typ in post:
            type_count[typ['type']] = type_count.get(typ['type'], 0) + 1
        return count, type_count

    #Compte le nombre de publications par auteur et trier le résultat par ordre croissant
    @classmethod
    def count_publi_authors(cls):
        cls.connexion()
        post = cls.col.aggregate([{'$unwind': '$authors'}, {'$group': { '_id': '$authors', 'count': {'$sum' : 1}}}, {'$sort': {'count': -1}}])
        classement = {}
        for elem in post:
            classement[elem['_id']] = elem['count']
        return classement

    @classmethod
    def inserer(cls):
        path = filedialog.askopenfilename()
        with open(path) as f:
            data = json.load(f)
        cls.client = MongoClient()
        cls.client.DBLP.test.insert_many(data)
        cls.deconnexion()
        valide = print('Fichier inséré en base !')
        return valide

    #Bien s'assurer que 'mongonimport.exe' soit dans le repertoire de fichiers pour que cela fonctionne
    @classmethod
    def inserer_2(cls, collect_name):
        cls.connexion()
        file = filedialog.askopenfilename()
        name = file.split('/')[-1]
        subprocess.call(f"mongoimport --db DBLP --collection {collect_name} --type json --file {file}", shell=True)
        cls.deconnexion()
        return print(f"Le fichier {name} a bien été inséré dans la collection {collect_name}.")


    #Fonctionne mais calcul extrêmement long !!!
    '''@classmethod
    def count_publi_authors(cls):
        liste_authors = cls.authors()
        liste_authors = list(liste_authors)
        liste_authors.sort()
        print(liste_authors)
        classement = {}
        for author in liste_authors:
            post = cls.col.find({'authors': author}).count()
            classement[author] = post
            print(author, post)
        classement = {k: v for k, v in sorted(classement.items(), key=lambda item: item[1])}
        return classement'''


# comptage = Connecteur.count_db()
# print("Le nombre de documents de la collection : ", comptage)

# liste_books = Connecteur.list_books()
# print("les livres de type Book : ", liste_books)

# book_2014 = Connecteur.after_2014()
# print("les livres depuis 2014 : ", book_2014)

# book_toru = Connecteur.publi_Toru()
# print("Les publications de l’auteur Toru Ishida : ", book_toru)

# liste_auteurs = Connecteur.authors()
# print("Auteurs distincts", liste_auteurs)

# book_sorted_toru = Connecteur.sort_Toru()
# print("Publications de Toru Ishida trié : ", book_sorted_toru)

# nb_publi_toru = Connecteur.count_book_Toru()
# print("Nombre de publications de Toru Ishida : ", nb_publi_toru)

# nb_2011_type = Connecteur.count_2001_type()
# print("Nombre de publications depuis 2011 : ", nb_2011_type[0], "\n Répartition :", nb_2011_type[1])

# classement_publication = Connecteur.count_publi_authors()
# print('Classement publication', classement_publication)

#Connecteur.inserer()

#Connecteur.inserer_2('test')