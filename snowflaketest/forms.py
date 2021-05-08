from django import forms

from snowflaketest.models import student


class studentfrom(forms.ModelForm):
    class Meta:
        model = student
        fields = ["NAME","PLACE"]